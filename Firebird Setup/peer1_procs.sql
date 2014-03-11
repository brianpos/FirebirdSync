SET TERM ^;
--
-- ========================================================================================================================================================================
--  PEER 1 - Stored Procs
-- ========================================================================================================================================================================
--
--
--  ********************************************************************
--     Select Incremental Changes Procs for orders and order_details
--  ********************************************************************
--
--  ***********************************************
--     Insert Procs for orders and order_details
--  ***********************************************
--
^
recreate procedure sp_orders_applyinsert
(
	order_id int,
	order_date date
)
RETURNS
(
	sync_row_count int
)
AS
BEGIN
	insert into orders (order_id, order_date) 
		values (:order_id, :order_date);
	sync_row_count = ROW_COUNT;
	--  EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;	
END;
^

recreate procedure sp_order_details_applyinsert
(
	order_id int,
	order_details_id int,
	product varchar(100),
	quantity int
)
returns
(
	sync_row_count int
)
AS
BEGIN
	insert into order_details (order_id, order_details_id, product, quantity) 
		values (:order_id, :order_details_id, :product, :quantity);
	sync_row_count = ROW_COUNT;
	--  EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
^

recreate procedure sp_orders_insert_md
(
	order_id int,
	sync_scope_local_id int,
	sync_row_is_tombstone int,
	sync_create_peer_key int,
	sync_create_peer_timestamp Integer,
	sync_update_peer_key int,
	sync_update_peer_timestamp Integer,
	sync_check_concurrency int,
	sync_row_timestamp Integer
)
returns
(
	sync_row_count int
)
AS
	DECLARE p_order_id int;
	DECLARE timestamp_for_this_change Integer;
BEGIN
	timestamp_for_this_change = gen_id(sequence_timestamp, 1);
	p_order_id = :order_id;

	update orders_tracking set 
		create_scope_local_id = :sync_scope_local_id, 
		scope_create_peer_key = :sync_create_peer_key,
		scope_create_peer_timestamp = :sync_create_peer_timestamp,
		local_create_peer_key = 0,
		local_create_peer_timestamp = :timestamp_for_this_change,
		update_scope_local_id = :sync_scope_local_id,
		scope_update_peer_key = :sync_update_peer_key,
		scope_update_peer_timestamp = :sync_update_peer_timestamp,
		local_update_peer_key = 0,
		local_update_peer_timestamp = :timestamp_for_this_change,
		sync_row_is_tombstone = :sync_row_is_tombstone 
	where order_id = :p_order_id
		and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);

	sync_row_count = ROW_COUNT;

	if (sync_row_count = 0) then
	begin
		-- inserting metadata for row if it does not already exist
		-- this can happen if a node sees a delete for a row it never had, we insert only metadata
		-- for the row in that case
		insert into orders_tracking (	
			order_id,
			create_scope_local_id, 
			scope_create_peer_key,
			scope_create_peer_timestamp,
			local_create_peer_key,
			local_create_peer_timestamp,
			update_scope_local_id,
			scope_update_peer_key,
			scope_update_peer_timestamp,
			local_update_peer_key,
			local_update_peer_timestamp,
			sync_row_is_tombstone )
		values (
			:p_order_id,
			:sync_scope_local_id, 
			:sync_create_peer_key,
			:sync_create_peer_timestamp,
			0,
			:timestamp_for_this_change,
			:sync_scope_local_id,
			:sync_update_peer_key,
			:sync_update_peer_timestamp,
			0,
			:timestamp_for_this_change,
			:sync_row_is_tombstone);
		
		sync_row_count = ROW_COUNT;
	end
END;
^

recreate procedure sp_order_details_insert_md
(
    order_details_id int,
	sync_scope_local_id int,
	sync_row_is_tombstone int,
	sync_create_peer_key int,
	sync_create_peer_timestamp Integer,
	sync_update_peer_key int,
	sync_update_peer_timestamp Integer,
	sync_check_concurrency int,
	sync_row_timestamp Integer
)
returns
(
	sync_row_count int
)
AS
	DECLARE p_order_details_id int;
	DECLARE timestamp_for_this_change Integer;
BEGIN
	timestamp_for_this_change = gen_id(sequence_timestamp, 1);
	p_order_details_id = :order_details_id;
  
    update order_details_tracking set 
		create_scope_local_id = :sync_scope_local_id, 
		scope_create_peer_key = :sync_create_peer_key,
		scope_create_peer_timestamp = :sync_create_peer_timestamp,
		local_create_peer_key = 0,
		local_create_peer_timestamp = :timestamp_for_this_change,
		update_scope_local_id = :sync_scope_local_id,
		scope_update_peer_key = :sync_update_peer_key,
		scope_update_peer_timestamp = :sync_update_peer_timestamp,
		local_update_peer_key = 0,
		local_update_peer_timestamp = :timestamp_for_this_change,
		sync_row_is_tombstone = :sync_row_is_tombstone 
    where order_details_id = :p_order_details_id
    and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
    
	sync_row_count = ROW_COUNT;

	if (sync_row_count = 0) then
	begin
		-- inserting metadata for row if it does not already exist
		-- this can happen if a node sees a delete for a row it never had, we insert only metadata
		-- for the row in that case
		insert into order_details_tracking 
		(	
            order_details_id,
			create_scope_local_id, 
			scope_create_peer_key,
			scope_create_peer_timestamp,
			local_create_peer_key,
			local_create_peer_timestamp,
			update_scope_local_id,
			scope_update_peer_key,
			scope_update_peer_timestamp,
			local_update_peer_key,
			local_update_peer_timestamp,
			sync_row_is_tombstone )
		values (
            :p_order_details_id,
			:sync_scope_local_id, 
			:sync_create_peer_key,
			:sync_create_peer_timestamp,
			0,
			:timestamp_for_this_change,
			:sync_scope_local_id,
			:sync_update_peer_key,
			:sync_update_peer_timestamp,
			0,
			:timestamp_for_this_change,
			:sync_row_is_tombstone);
    
		sync_row_count = ROW_COUNT;
    END
END;
^

--
--  ***********************************************
--     Update Procs for orders and order_details
--  ***********************************************
--
recreate procedure sp_orders_applyupdate
(
    order_id int,
    order_date date,
    sync_force_write int,
    sync_min_timestamp Integer
)
returns
(
    sync_row_count int
)        
AS
BEGIN
    update orders o set 
        order_date = :order_date
    where o.order_id in
    (
        select order_id from orders_tracking t
        where (t.local_update_peer_timestamp <= :sync_min_timestamp or :sync_force_write = 1)       
        and t.order_id = :order_id
    );
    
	sync_row_count = ROW_COUNT;
END;                  		
^

recreate procedure sp_order_details_applyupdate
(
        order_id int,
        order_details_id int,
        quantity int,
        product varchar(100),
		sync_force_write int,
		sync_min_timestamp Integer
)
returns
(
		sync_row_count int
)        
AS
BEGIN
    update order_details o set 
        o.order_id = :order_id,
        o.product = :product,
        o.quantity = :quantity  
    where o.order_details_id in
    (
        select order_details_id from order_details_tracking t
        where (t.local_update_peer_timestamp <= :sync_min_timestamp or :sync_force_write = 1)       
        and t.order_details_id = :order_details_id
    );

    sync_row_count = ROW_COUNT;
END;                  		
^

recreate procedure sp_orders_update_md
(
    order_id int,
    sync_scope_local_id int,
    sync_row_timestamp Integer,
    sync_create_peer_key int ,
    sync_create_peer_timestamp Integer,                 
    sync_update_peer_key int,
    sync_update_peer_timestamp Integer,                      
    sync_row_is_tombstone int,
    sync_check_concurrency int
)
returns
(
	sync_row_count int
)
AS
    DECLARE p_order_id int;
    DECLARE was_tombstone int;
	DECLARE timestamp_for_this_change Integer;
BEGIN
	timestamp_for_this_change = gen_id(sequence_timestamp, 1);
    p_order_id = :order_id;

	was_tombstone = (select sync_row_is_tombstone from orders_tracking 
	where order_id = :p_order_id);
	
	if (:was_tombstone is not null and :was_tombstone=1 and :sync_row_is_tombstone=0) then
	begin
		-- tombstone is getting resurrected, update creation version as well
		update orders_tracking set
			update_scope_local_id = :sync_scope_local_id, 
            scope_update_peer_key = :sync_update_peer_key,
            scope_update_peer_timestamp = :sync_update_peer_timestamp,
            local_update_peer_key = 0,
            local_update_peer_timestamp = :timestamp_for_this_change,
            create_scope_local_id = :sync_scope_local_id, 
            scope_create_peer_key = :sync_create_peer_key, 
            scope_create_peer_timestamp =  :sync_create_peer_timestamp, 
            sync_row_is_tombstone = :sync_row_is_tombstone 						
		where order_id = :p_order_id
		and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
	end
	else
	begin	
		update orders_tracking set
			update_scope_local_id = :sync_scope_local_id, 
            scope_update_peer_key = :sync_update_peer_key,
            scope_update_peer_timestamp = :sync_update_peer_timestamp,
            local_update_peer_key = 0,
            local_update_peer_timestamp = :timestamp_for_this_change,
            sync_row_is_tombstone = :sync_row_is_tombstone 						
		where order_id = :p_order_id 			
		and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
    end
    sync_row_count = ROW_COUNT;
END
^	

recreate procedure sp_order_details_update_md
(
    order_details_id int,
    sync_scope_local_id int,
    sync_row_timestamp Integer,
    sync_create_peer_key int ,
    sync_create_peer_timestamp Integer,                 
    sync_update_peer_key int,
    sync_update_peer_timestamp Integer,                      
    sync_row_is_tombstone int,
    sync_check_concurrency int
)
returns
(
	sync_row_count int
)
AS
    DECLARE was_tombstone int;
	DECLARE timestamp_for_this_change Integer;
BEGIN
	timestamp_for_this_change = gen_id(sequence_timestamp, 1);

	was_tombstone = (select sync_row_is_tombstone from order_details_tracking 
	where order_details_id = :order_details_id);
	
	if (:was_tombstone is not null and :was_tombstone=1 and :sync_row_is_tombstone=0) then
	begin
		-- tombstone is getting resurrected, update creation version as well
		update order_details_tracking set
			update_scope_local_id = :sync_scope_local_id, 
            scope_update_peer_key = :sync_update_peer_key,
            scope_update_peer_timestamp = :sync_update_peer_timestamp,
            local_update_peer_key = 0,
            local_update_peer_timestamp = :timestamp_for_this_change,
            create_scope_local_id = :sync_scope_local_id, 
            scope_create_peer_key = :sync_create_peer_key, 
            scope_create_peer_timestamp =  :sync_create_peer_timestamp, 
            sync_row_is_tombstone = :sync_row_is_tombstone 						
		where order_details_id = :order_details_id
		and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
	end
	else
	begin	
		update order_details_tracking set
			update_scope_local_id = :sync_scope_local_id, 
            scope_update_peer_key = :sync_update_peer_key,
            scope_update_peer_timestamp = :sync_update_peer_timestamp,
            local_update_peer_key = 0,
            local_update_peer_timestamp = :timestamp_for_this_change,
            sync_row_is_tombstone = :sync_row_is_tombstone 						
		where order_details_id = :order_details_id 			
		and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
    end
    sync_row_count = ROW_COUNT;
END
^	

--
--  ***********************************************
--     Delete Procs for orders and order_details
--  ***********************************************
--
^
recreate procedure sp_orders_applydelete
(
    order_id int,
    sync_min_timestamp Integer,
    sync_force_write int
)
returns
(
    sync_row_count int
)
AS
BEGIN
    delete from orders o 
    where o.order_id in 
    (select t.order_id from orders_tracking t
      where (t.local_update_peer_timestamp <= :sync_min_timestamp  or :sync_force_write = 1)      
      and t.order_id = :order_id);  
    sync_row_count = ROW_COUNT;
END;                 
^

recreate procedure sp_order_details_applydelete
(
    order_details_id int,
    sync_min_timestamp Integer,
    sync_force_write int
)
returns
(
    sync_row_count int
)
AS
BEGIN
    delete from order_details o
    where o.order_details_id in 
    (select t.order_details_id from order_details_tracking t
      where (t.local_update_peer_timestamp <= :sync_min_timestamp  or :sync_force_write = 1)      
      and t.order_details_id = :order_details_id);  
    sync_row_count = ROW_COUNT;
END;                 
^

recreate procedure sp_orders_delete_md
(
    order_id int,
    sync_row_timestamp Integer,	
    sync_check_concurrency int
)
returns
(
		sync_row_count int
)        
AS
BEGIN
	-- delete metadata only
	delete from orders_tracking o
	where o.order_id = :order_id and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
    sync_row_count = ROW_COUNT;
END;
^

recreate procedure sp_order_details_delete_md
(
    order_details_id int,
    sync_row_timestamp Integer,	
    sync_check_concurrency int
)
returns
(
		sync_row_count int
)        
AS
BEGIN
	-- delete metadata only
	delete from order_details_tracking o
	where o.order_details_id = :order_details_id and (:sync_check_concurrency = 0 or local_update_peer_timestamp = :sync_row_timestamp);
    sync_row_count = ROW_COUNT;
END;
^

--
--  ***********************************************
--     Get conflicting row procs
--  ***********************************************
--
^
recreate procedure sp_orders_selectrow
(
	p_order_id int,
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
BEGIN
    FOR
	select
		t.order_id,
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
	from orders o right join orders_tracking t on o.order_id = t.order_id
	where t.order_id = :p_order_id
	into
		:order_id,
		:order_date,
		:sync_row_is_tombstone,
		:sync_row_timestamp,
		:sync_update_peer_timestamp,
		:sync_update_peer_key,
		:sync_create_peer_timestamp,
		:sync_create_peer_key
    DO
    BEGIN
        suspend;
    END
END;
^

recreate procedure sp_order_details_selectrow
(
    p_order_details_id int,
    sync_scope_local_id int
)
returns
(
    order_id int,
    order_details_id int,
    product varchar(100),
    quantity int,

	sync_row_is_tombstone int,
	sync_row_timestamp Integer,
	sync_update_peer_timestamp Integer,
	sync_update_peer_key int,
	sync_create_peer_timestamp Integer,
	sync_create_peer_key int
)
AS
BEGIN
    for 
    select	
        o.order_id, 
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
    where t.order_details_id = :p_order_details_id
	into
		:order_id,
		:order_details_id,
		:product,
		:quantity,
		:sync_row_is_tombstone,
		:sync_row_timestamp,
		:sync_update_peer_timestamp,
		:sync_update_peer_key,
		:sync_create_peer_timestamp,
		:sync_create_peer_key
    DO
    BEGIN
        suspend;
    END
END;
^

--
--  ***********************************************
--     Get tombstones for cleanup commands
--  ***********************************************
--

recreate procedure sp_orders_select_ts
(
	tombstone_aging_in_hours int,
	sync_scope_local_id int
)
returns
(
    order_id int,
    sync_row_timestamp Integer,
    sync_update_peer_timestamp Integer,
    sync_update_peer_key int,
    sync_create_peer_timestamp Integer,
    sync_create_peer_key int
)
AS
BEGIN
	for select
        order_id,
        local_update_peer_timestamp as sync_row_timestamp,  
        case when (update_scope_local_id is null or update_scope_local_id <> :sync_scope_local_id) 
             then local_update_peer_timestamp else scope_update_peer_timestamp end as sync_update_peer_timestamp,
        case when (update_scope_local_id is null or update_scope_local_id <> :sync_scope_local_id) 
             then local_update_peer_key else scope_update_peer_key end as sync_update_peer_key,
        case when (create_scope_local_id is null or create_scope_local_id <> :sync_scope_local_id) 
             then local_create_peer_timestamp else scope_create_peer_timestamp end as sync_create_peer_timestamp,
        case when (create_scope_local_id is null or create_scope_local_id <> :sync_scope_local_id) 
             then local_create_peer_key else scope_create_peer_key end as sync_create_peer_key			
	from orders_tracking 
	where sync_row_is_tombstone=1 
	and (current_timestamp - last_change_datetime) > :tombstone_aging_in_hours
    into
        :order_id,
        :sync_row_timestamp,
        :sync_update_peer_timestamp,
        :sync_update_peer_key,
        :sync_create_peer_timestamp,
        :sync_create_peer_key			
    DO
    BEGIN
        suspend;
    END
END;
^

recreate procedure sp_order_details_select_ts
(
	tombstone_aging_in_hours int,
	sync_scope_local_id int
)
returns
(
    order_details_id int,
    sync_row_timestamp Integer,
    sync_update_peer_timestamp Integer,
    sync_update_peer_key int,
    sync_create_peer_timestamp Integer,
    sync_create_peer_key int
)
AS
BEGIN
	for select 
        order_details_id,
        local_update_peer_timestamp as sync_row_timestamp, 
        case when (update_scope_local_id is null or update_scope_local_id <> :sync_scope_local_id) 
             then local_update_peer_timestamp else scope_update_peer_timestamp end as sync_update_peer_timestamp,
        case when (update_scope_local_id is null or update_scope_local_id <> :sync_scope_local_id) 
             then local_update_peer_key else scope_update_peer_key end as sync_update_peer_key,
        case when (create_scope_local_id is null or create_scope_local_id <> :sync_scope_local_id) 
             then local_create_peer_timestamp else scope_create_peer_timestamp end as sync_create_peer_timestamp,
        case when (create_scope_local_id is null or create_scope_local_id <> :sync_scope_local_id) 
             then local_create_peer_key else scope_create_peer_key end as sync_create_peer_key			
	from order_details_tracking 
	where sync_row_is_tombstone=1 
	and (current_timestamp - last_change_datetime) > :tombstone_aging_in_hours
    into
        :order_details_id,
        :sync_row_timestamp,
        :sync_update_peer_timestamp,
        :sync_update_peer_key,
        :sync_create_peer_timestamp,
        :sync_create_peer_key			
    DO
    BEGIN
        suspend;
    END
END;	
^
SET TERM ;^
