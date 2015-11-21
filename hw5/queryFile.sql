/*Warmup problems */

/*Your first Transaction*/

set transaction read write;


/*Code goes here*/
SELECT s.name, s.age FROM s;
SELECT * FROM data;
commit;

/*Update, Insert, Delete: Part 1: insert entries*/
set transaction read write;


/*Code goes here*/
INSERT INTO data (f1,f2) VALUES (6,60);
INSERT INTO data (f1,f2) VALUES (10,33);
INSERT INTO s (sid, age, name) VALUES (6, 6, 'Michael');
INSERT INTO s (sid, age, name) VALUES (7, 60, 'Michelle');
commit;

/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/

/*Part 2: update entries*/
/*Put everything in 1 transaction, as seen in above part 1*/
set transaction read write;


/*Code goes here*/
UPDATE data SET data.f2=1 WHERE data.f1=1;
UPDATE s SET s.age=100 WHERE s.name='Michelle';
UPDATE data SET data.f2=100 WHERE data.f2=50;
commit;

/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/

/*Part 3: delete entries*/
/*Put everything in 1 transaction, as seen in above part 1*/
set transaction read write;


/*Code goes here*/
DELETE data WHERE data.f1=2;
DELETE s;
commit;

/*========please do not remove=========*/
select * from data;
select * from s;
/*===================================*/
