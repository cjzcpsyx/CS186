/*Warmup problems */

/*Your first Transaction*/

set transaction read write;


/*Code goes here*/


commit;

/*Update, Insert, Delete: Part 1: insert entries*/
set transaction read write;


/*Code goes here*/


commit;

/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/

/*Part 2: update entries*/
/*Put everything in 1 transaction, as seen in above part 1*/



/*Code goes here*/



/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/

/*Part 3: delete entries*/
/*Put everything in 1 transaction, as seen in above part 1*/



/*Code goes here*/



/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/
