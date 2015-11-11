/* Test Rollback and Recovery Here */

set transaction read write;



/* update table and end transaction above */

select * from data;
select * from s;