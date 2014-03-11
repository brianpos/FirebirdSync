-- ========================================================================================================================================================================
--  Configure scope Memebers
-- ========================================================================================================================================================================
delete from orders;
delete from orders_tracking;
delete from order_details;
delete from order_details_tracking;
delete from scope_info;
delete from scope_table_map;

-- recreate the basic configuration
insert into scope_info(scope_name) values ('sales');
insert into scope_table_map(scope_name, table_name) values ('sales', 'orders');
insert into scope_table_map(scope_name, table_name) values ('sales', 'order_details');

-- ========================================================================================================================================================================
--  Insert Sample Data In Tables
-- ========================================================================================================================================================================

insert into orders (order_id, order_date) values(1, current_timestamp);
insert into orders (order_id, order_date) values(4, current_timestamp);

insert into order_details (order_id, order_details_id, product, quantity) values(1, 1 , 'DVD', 5);
insert into order_details (order_id, order_details_id, product, quantity) values(1, 2 , 'CD', 10);
insert into order_details (order_id, order_details_id, product, quantity) values(4, 3 , 'Floppy Disk', 15);

-- ========================================================================================================================================================================
--  Test case 1 to perform a set of updates
-- ========================================================================================================================================================================
update orders set order_date = '2014-3-1' where order_id = 4;
update order_details set quantity = 13 where order_details_id = 3;
delete from orders where order_id = 1;
delete from order_details where order_id = 1;

-- ========================================================================================================================================================================
--  Test case 2 to perform another set of updates (so can target more than 1 replica and view differences)
-- ========================================================================================================================================================================
update orders set order_date = '2014-3-2' where order_id = 4;
update order_details set quantity = 9 where order_details_id = 3;

insert into orders (order_id, order_date) values(2, current_timestamp);

insert into order_details (order_id, order_details_id, product, quantity) values(2, 4 , 'DVD', 5);
insert into order_details (order_id, order_details_id, product, quantity) values(2, 5 , 'CD', 10);
insert into order_details (order_id, order_details_id, product, quantity) values(2, 6 , 'Floppy Disk', 15);


-- ========================================================================================================================================================================
--  Bulk Insert Data In Tables
-- ========================================================================================================================================================================
/*declare 
 p_index int := 0;
begin
while (p_index < 1000) LOOP
  BEGIN
	insert into orders(order_id, order_Date) values (p_index, SYSDATE);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;
  BEGIN
	insert into order_details(order_details_id,order_id, product,quantity) values (p_index+1000,p_index, 'aaaa', 100);
       EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;
	p_index := p_index + 1;
END LOOP;
end;


-- ========================================================================================================================================================================
--  Cleanup content and metadata
-- ========================================================================================================================================================================
delete orders;
delete orders_tracking;
delete order_details;
delete order_details_tracking;

-- ========================================================================================================================================================================
--  DML - Test Code
-- ========================================================================================================================================================================
insert into orders (order_id, order_date) values(1, current_timestamp);
insert into orders (order_id, order_date) values(4, current_timestamp);

update orders set order_date = '2014-3-1' where order_id = 4;
update order_details set quantity = 13 where order_details_id = 3;
delete orders where order_id = 1;
delete from order_details where order_id = 1;


insert into order_details (order_id, order_details_id, product, quantity) values(1, 1 , 'DVD', 5);
--insert into order_details (order_id, order_details_id, product, quantity) values(1, 1 , 'CD', 10);
insert into order_details (order_id, order_details_id, product, quantity) values(4, 4 , 'Floppy Disk', 15);

select * from orders;

select * from orders_tracking;
select * from order_details;
select * from order_details_tracking;
select * from scope_info;
*/
