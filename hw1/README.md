# Homework 1: Real-world SQL queries
#### Note: *This homework is to be done individually!*
#### Due: Friday, September 11, 2015, 11:59 PM

###Description

In this homework, we will exercise your newly acquired SQL skills. You will be writing queries for public data stored in a SQLite3 database. To give context to how you may use these queries in a Java application, we will be making these queries using the JDBC API.

###Getting started
To obtain the homework files, pull this repository from your own repository. HW0 should help, should you forget how to do this.

On your local machine, download the dataset from the [Yelp Dataset Challenge](http://www.yelp.com/dataset_challenge) and extract the files into your hw1 directory. 

Run the included `json_to_db.py` file in your hw1 directory. This script converts the Yelp dataset from JSON to a database that we can query using SQL. Feel free to take a look to get a feel for what table creation and record insertion queries should look like, although you won't need to use those in this assignment.

####Important
As per the dataset terms of use, please **do not upload or redistribute any of the JSON files and the .db file generated above**. You will lose points on the assignment if you do so, so check your committed files carefully! 

#####SQLite3
Finally, download and install [SQLite3](https://www.sqlite.org/download.html) as a command-line shell program. For those with OSX, sqlite3 is likely already installed! You can verify by running `sqlite3` in your terminal. 

Now that you have sqlite3, you can start by running `sqlite3 yelp_dataset.db` in your terminal. Or if you're already in sqlite3,
load your database using `.open yelp_dataset.db`. To view the schema, type `.schema`. You should also try to run a simple SQL query.

###Schema
A schema ["refers to the organization of data as a blueprint of how a database is constructed"](https://en.wikipedia.org/wiki/Database_schema). It essentially describes the columns of our database tables.

The schema for our database is slightly different than the json format of the original dataset. First, peruse the schema before starting your queries. The easiest way to do this is with `.schema`. However, it is also possible to print your schema via JDBC. To do so, take a look at `getMetaData` method of the `Connection` class, and the `getTables` and `getColumns` methods of the `DatabaseMetaData` class.  

####Caveats and Tips (May be updated as we go)
1. The checkins table stores data about the total number of check-ins that a business has on a certain day of the week (for example, Sunday). The `day` attribute of the table represents the day of the week as an integer. Sunday is 0, Monday is 1, Tuesday is 2, and so on. 

1. Dates are in the format of 'YYYY-MM'.

1. If two users, X and Y, are friends on Yelp, the `friends` table will contain two entries for that friendship. `user1_id` of one entry will be the user ID of user X, and `user2_id` will be the user ID of user Y. There will also be an entry where `user1_id` will be the user ID of user Y and `user2_id` will be the user ID of userX.

1. More details about the dataset can be found on the dataset page.

1. Capitalization is neat but [not necessary.](http://stackoverflow.com/questions/292026/is-there-a-good-reason-to-use-upper-case-for-sql-keywords)

1. Some of these queries may be very intellectually challenging! Be sure to use Google in an academically honest way if you ever get stuck.

1. Dataset is a subset of the entire yelp database and is not holistically consistent - there may be numbers that don't add up among tables, and it's ok.

###Writing queries
Now that we've explored the structure of our database, let's start writing queries! Please write your solution for each of the queries below in the `YelpQueries.java` file included in the hw1 directory. 
For example, consider a hypothetical question 0: "What is the total number of reviews in our dataset?" It should look like this:

    // q0 
    String q0 = "CREATE VIEW q0 AS " 
                 + "SELECT count(*) FROM reviews";
    statement.execute(q0);

Create views for the following queries:

1. Find the average number of reviews left by all users who have rated less than 10 times.
2. What are the names of the users created after November 2014 and have left more than 50 reviews?
3. What are the names and stars of all businesses rated higher than 3 stars in Pittsburgh?
4. What is the name of the lowest rated business in Las Vegas with at least 500 reviews?
5. What are the names of the 5 businesses that have the most check-ins on Sunday?
6. What day of the week (integer) do people check-in the most on?
7. Find the names of the businesses reviewed by the user who has made the most reviews.
8. Find the average stars of the top 10% most rated businesses in Edinburgh.
9. What are the names of the users that have ‘..’ in their name?
10. (Follow up to question 9) Which city has the most businesses that have been rated by these people?


###Testing
To run the test, from within the `hw1` directory:

	$ bash test.sh
	
Become familiar with the UNIX [diff](http://en.wikipedia.org/wiki/Diff) command, if you're not already, because our tests saves the `diff` for any query executions that don't match in `diffs/`.  If you care to look at the query outputs directly, ours are located in the `expected_output` directory. Your view output should be located in your solution's `your_output` directory once you run the tests.

**Note:** It doesn't matter how you sort your results; we will reorder them before comparing. Note, however, that our test query output is sorted, so if you're trying to compare yours and ours manually line-by-line, make sure you use the proper ORDER BY clause. The ORDER BY clause to use can be determined by looking in `test.sh`. 

###Submitting
To submit, push to the `release/hw1` branch on GitHub. We will only grade what is in this release branch. **Remember: do not commit the json or .db files!**

