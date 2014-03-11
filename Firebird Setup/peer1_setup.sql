SET TERM ^;

--drop table scope_info^
--drop table scope_table_map^
--drop sequence sequence_newid^
--drop sequence sequence_timestamp^
--drop procedure sp_get_timestamp^
--drop procedure sp_select_shared_scopes^
--drop table orders^
--drop table order_details^
--drop table orders_tracking^
--drop table order_details_tracking^


-----------------------------------------------------------------------------
-- Create scope info table
-- This stores the scopes that have been created for all replicas
--
-- The timestamps that are used in the sync are actually just a sequence
-- number of all updates in this system used for sequencing the sync.
-----------------------------------------------------------------------------
RECREATE TABLE scope_info
(
    scope_local_id Integer, 
    scope_id char(16) CHARACTER SET OCTETS UNIQUE NOT NULL, 
    scope_name varchar(100) NOT NULL PRIMARY KEY, 
    scope_sync_knowledge blob,
    scope_forgotten_knowledge blob, 
    scope_timestamp Integer, 
    scope_cleanup_timestamp Integer
)^

CREATE SEQUENCE sequence_newid^
CREATE SEQUENCE sequence_timestamp^

-----------------------------------------------------------------------------
-- Create scope table mapping table
-----------------------------------------------------------------------------
RECREATE TABLE scope_table_map
(    
    scope_name varchar(100) ,
    table_name varchar(100)     
)^

create unique index clustered_scope_table_map
on scope_table_map(scope_name, table_name)^

-----------------------------------------------------------------------------
-- Trigger when inserting a row into the scope_info table to update 
-- the ids and the timestamp
-----------------------------------------------------------------------------
RECREATE TRIGGER newid_trigger for scope_info
active before insert position 0
as
begin
    if (new.scope_local_id is null) then
        new.scope_local_id = gen_id(sequence_newid, 1);
    if (new.scope_id is null) then
        new.scope_id = gen_uuid();
    new.scope_timestamp = gen_id(sequence_timestamp, 1);
end^

-----------------------------------------------------------------------------
-- Stored Procedure to update the Scope_info table
-- (this was done as the text approach didn't like having mutliple statements)
-----------------------------------------------------------------------------
RECREATE PROCEDURE SP_UPDATE_SCOPE_INFO
(
    sync_scope_knowledge	Blob,
    sync_scope_id	char(16) CHARACTER SET OCTETS,
    sync_scope_cleanup_knowledge	Blob,
    sync_scope_name	VarChar(100),
    sync_check_concurrency	Integer,
    sync_scope_timestamp	Integer
) 
RETURNS 
(
    sync_row_count	Integer
)
AS 
BEGIN
    /* write your code here */ 
    update scope_info 
    set   scope_sync_knowledge = :sync_scope_knowledge, 
        scope_id = :sync_scope_id, 
        scope_forgotten_knowledge = :sync_scope_cleanup_knowledge 
    where scope_name = :sync_scope_name 
        and ( :sync_check_concurrency = 0 or scope_timestamp = :sync_scope_timestamp);
    sync_row_count = ROW_COUNT;
    suspend;
END^

-----------------------------------------------------------------------------
-- Get the next available timestamp
-----------------------------------------------------------------------------
RECREATE PROCEDURE sp_get_timestamp
RETURNS ( SYNC_NEW_TIMESTAMP int )
AS
BEGIN
  sync_new_timestamp = gen_id(sequence_newid, 1);
  suspend;
END^

-----------------------------------------------------------------------------
-- Create a procedure that identifies where tables have a shared scope
-----------------------------------------------------------------------------
RECREATE PROCEDURE sp_select_shared_scopes
(
    sync_scope_name varchar(100)
)        
RETURNS (
    sync_table_name varchar(100),
    sync_shared_scope_name varchar(100))
AS
BEGIN
    FOR 
        select  scopeTableMap2.table_name as sync_table_name, 
                scopeTableMap2.scope_name as sync_shared_scope_name
        from scope_table_map scopeTableMap1 
        join scope_table_map scopeTableMap2
            on scopeTableMap1.table_name = scopeTableMap2.table_name
            and scopeTableMap1.scope_name = :sync_scope_name
        where scopeTableMap2.scope_name <> :sync_scope_name
        INTO :sync_table_name, :sync_shared_scope_name
    DO
    BEGIN
        SUSPEND;
    END
END^

-----------------------------------------------------------------------------
-- Create the sample data tables (orders and order_details)
-----------------------------------------------------------------------------
RECREATE TABLE orders
(
    order_id Integer NOT NULL primary key, 
    order_date date
)^

RECREATE TABLE order_details
(
    order_id Integer NOT NULL, 
    order_details_id Integer NOT NULL primary key, 
    product varchar(100),
    quantity Integer
)^

-----------------------------------------------------------------------------
-- Create tracking tables (linked to the 2 sample tables)
-----------------------------------------------------------------------------
RECREATE TABLE orders_tracking
(
    order_id int NOT NULL primary key,
    update_scope_local_id int, 
    scope_update_peer_key int,
    scope_update_peer_timestamp Integer,
    local_update_peer_key int,
    local_update_peer_timestamp Integer,
    create_scope_local_id int,
    scope_create_peer_key int,
    scope_create_peer_timestamp Integer,
    local_create_peer_key int,
    local_create_peer_timestamp Integer,
    sync_row_is_tombstone int, 
    touch_timestamp Integer, 
    last_change_datetime timestamp default NULL
)^
 
-- Create index on orders tracking table
CREATE INDEX NC_Orders_tracking_ts_index
ON orders_tracking(local_update_peer_timestamp)^
 
 
-- Create order_details tracking tables
RECREATE TABLE order_details_tracking
(
    order_details_id int NOT NULL primary key,    
    update_scope_local_id int, 
    scope_update_peer_key int,
    scope_update_peer_timestamp Integer,
    local_update_peer_key int,
    local_update_peer_timestamp Integer,
    create_scope_local_id int,
    scope_create_peer_key int,
    scope_create_peer_timestamp Integer,
    local_create_peer_key int,
    local_create_peer_timestamp Integer,
    sync_row_is_tombstone int, 
    touch_timestamp Integer, 
    last_change_datetime timestamp default NULL
)^

-- Create index on order details tracking table
CREATE INDEX order_details_tracking_index
ON order_details_tracking (local_update_peer_timestamp)^


-----------------------------------------------------------------------------
-- Create Triggers to maintain the tracking tables from changes
-- made to the orders and order_detaild tables
-----------------------------------------------------------------------------

-- inserts a new metadata row if one does not exist
-- if the row existed before and was deleted, the tombstone row is got back live.
RECREATE TRIGGER orders_insert_trigger FOR orders active
AFTER INSERT
as
	DECLARE VARIABLE timestamp_for_this_change Integer;
-- NOTE: order of UPDATE, INSERT below is important
-- resurrect the tombstone row, do not update creation version
BEGIN
    timestamp_for_this_change = gen_id(sequence_timestamp, 1);
    update orders_tracking ot 
    set 
      sync_row_is_tombstone = 0, local_update_peer_key = 0, 
      local_update_peer_timestamp = :timestamp_for_this_change, update_scope_local_id = NULL, last_change_datetime = current_timestamp 
    where ot.order_id = new.order_id;
    
    insert into orders_tracking
                (order_id, create_scope_local_id, 
        local_create_peer_key, local_create_peer_timestamp, 
        update_scope_local_id, local_update_peer_key, 
        local_update_peer_timestamp, sync_row_is_tombstone, last_change_datetime)
      values (new.order_id, NULL, 0, :timestamp_for_this_change, NULL, 0, :timestamp_for_this_change, 0, current_timestamp );
    -- EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
END
^

RECREATE TRIGGER order_details_insert_trigger for order_details
AFTER INSERT
AS
	DECLARE VARIABLE timestamp_for_this_change Integer;
    -- NOTE: order of UPDATE, INSERT below is important
    -- resurrect the tombstone row, do not update creation version
BEGIN
    timestamp_for_this_change = gen_id(sequence_timestamp, 1);
    update order_details_tracking odt 
    set 
        sync_row_is_tombstone = 0, local_update_peer_key = 0, 
        local_update_peer_timestamp = :timestamp_for_this_change, update_scope_local_id = NULL,
        last_change_datetime = current_timestamp
    where  odt.order_details_id = new.order_details_id;
    
    insert into order_details_tracking(order_details_id, create_scope_local_id, 
        local_create_peer_key, local_create_peer_timestamp, 
        update_scope_local_id, local_update_peer_key, 
        local_update_peer_timestamp, sync_row_is_tombstone, last_change_datetime)
    values (new.order_details_id, NULL, 0, :timestamp_for_this_change, NULL, 0, :timestamp_for_this_change, 0, current_timestamp);
    --EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
END
^

-- update triggers

-- update trigger sets the update_timestamp (gets automatically updated) and last_change_datetime values
RECREATE TRIGGER orders_update_trigger for orders
AFTER UPDATE
as
  BEGIN
    update orders_tracking t    
    set 
        update_scope_local_id = NULL, local_update_peer_key = 0, 
        local_update_peer_timestamp = gen_id(sequence_timestamp, 1), last_change_datetime = current_timestamp
    where t.order_id = new.order_id;      
  END
^

RECREATE TRIGGER order_details_update_trigger for order_details
AFTER UPDATE
AS
  BEGIN
    update order_details_tracking t    
    set 
        update_scope_local_id = NULL, local_update_peer_key = 0, 
        local_update_peer_timestamp = gen_id(sequence_timestamp, 1), last_change_datetime = current_timestamp
    where t.order_details_id = new.order_details_id;        
  END
^

-- delete triggers

-- delete trigger sets update_timestamp (gets automatically updated) and marks the row as deleted
RECREATE TRIGGER orders_delete_trigger for orders
AFTER DELETE
AS
begin
    update orders_tracking t 
        set 
            sync_row_is_tombstone = 1, update_scope_local_id = NULL, 
            local_update_peer_key = 0, local_update_peer_timestamp = gen_id(sequence_timestamp, 1),
            last_change_datetime = current_timestamp
        where t.order_id = old.order_id;
end
^

RECREATE TRIGGER order_details_delete_trigger for order_details
AFTER DELETE
AS
begin
    update order_details_tracking t 
        set 
            sync_row_is_tombstone = 1, update_scope_local_id = NULL, 
            local_update_peer_key = 0, local_update_peer_timestamp = gen_id(sequence_timestamp, 1),
            last_change_datetime = current_timestamp
       where t.order_details_id = old.order_details_id ;
end
^

create or alter procedure sp_orders_selectchanges
(
    sync_min_timestamp Integer,
    sync_metadata_only int,
    sync_scope_local_id int
)
returns
(
    order_id int,
    order_date date,
    
    sync_row_is_tombstone int,
    sync_row_timestamp Integer,
    
    sync_update_peer_timestamp Integer,
    sync_update_peer_key int,
    
    sync_create_peer_timestamp Integer,
    sync_create_peer_key int
)
AS
begin
    FOR
        select  t.order_id,
                o.order_date, 
                t.sync_row_is_tombstone,
                t.local_update_peer_timestamp as sync_row_timestamp,
                
                case when (t.update_scope_local_id is null or t.update_scope_local_id <> :sync_scope_local_id) 
                     then t.local_update_peer_timestamp else t.scope_update_peer_timestamp end as sync_update_peer_timestamp,
                     
                case when (t.update_scope_local_id is null or t.update_scope_local_id <> :sync_scope_local_id) 
                     then t.local_update_peer_key else t.scope_update_peer_key end as sync_update_peer_key,
                     
                case when (t.create_scope_local_id is null or t.create_scope_local_id <> :sync_scope_local_id) 
                     then t.local_create_peer_timestamp else t.scope_create_peer_timestamp end as sync_create_peer_timestamp,
                     
                case when (t.create_scope_local_id is null or t.create_scope_local_id <> :sync_scope_local_id) 
                     then t.local_create_peer_key else t.scope_create_peer_key end as sync_create_peer_key
        from orders o 
        right join orders_tracking t 
            on o.order_id = t.order_id
        where t.local_update_peer_timestamp > :sync_min_timestamp
        into :order_id,:order_date,
            :sync_row_is_tombstone, :sync_row_timestamp,
            :sync_update_peer_timestamp, :sync_update_peer_key,
            :sync_create_peer_timestamp, :sync_create_peer_key
    DO
    begin
        suspend;
    end
end
^


create or alter
procedure sp_order_details_selectchanges 
(
	sync_min_timestamp Integer,
	sync_metadata_only int,
	sync_scope_local_id int
)
returns
(
	order_id Integer, 
	order_details_id Integer , 
	product varchar(100),
	quantity Integer,
    
    sync_row_is_tombstone int,
    sync_row_timestamp Integer,
    
    sync_update_peer_timestamp Integer,
    sync_update_peer_key int,
    
    sync_create_peer_timestamp Integer,
    sync_create_peer_key int
)
AS
begin
    FOR
        select  o.order_id,
                t.order_details_id,
                o.product,
                o.quantity,
                t.sync_row_is_tombstone,
                t.local_update_peer_timestamp as sync_row_timestamp, 

                case when (t.update_scope_local_id is null or t.update_scope_local_id <> :sync_scope_local_id) 
                     then t.local_update_peer_timestamp else t.scope_update_peer_timestamp end as sync_update_peer_timestamp,

                case when (t.update_scope_local_id is null or t.update_scope_local_id <> :sync_scope_local_id) 
                     then t.local_update_peer_key else t.scope_update_peer_key end as sync_update_peer_key,

                case when (t.create_scope_local_id is null or t.create_scope_local_id <> :sync_scope_local_id) 
                     then t.local_create_peer_timestamp else t.scope_create_peer_timestamp end as sync_create_peer_timestamp,

                case when (t.create_scope_local_id is null or t.create_scope_local_id <> :sync_scope_local_id) 
                     then t.local_create_peer_key else t.scope_create_peer_key end as sync_create_peer_key
        from order_details o right join order_details_tracking t on o.order_details_id = t.order_details_id
        where t.local_update_peer_timestamp > :sync_min_timestamp
        into :order_id, :order_details_id , :product, :quantity,
            :sync_row_is_tombstone, :sync_row_timestamp,
            :sync_update_peer_timestamp, :sync_update_peer_key,
            :sync_create_peer_timestamp, :sync_create_peer_key
    DO
    begin
        suspend;
    end
END;
^

SET TERM ; ^
