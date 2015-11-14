SimpleDB: Recovery
=========================

This assignment is due **Tuesday, November 24th at 11:59 PM** and is worth **12% of your final grade**. We will start off by writing some SQL Queries for database updates, and then we will implement log-based rollback and crash recovery to enforce atomicity and durability.


## Part 1: SQL Queries Deja Vu

We have two tables - `data` and `s` (which stands for students). You will be working with the `queryFile.sql` file in the project directory. The schemas for these two tables are given below: 

```
data (f1 int, f2 int)
s (sid int, age int, name string)
```

You can start up a SimpleDB interactive console by running the following command: `bash scripts/db.sh`.  Note that this will not persist your data - you can run a session to actually store your data with the following commands:

1. After compiling with `ant`, run the following two commands once:
  1. `java -jar dist/simpledb.jar convert data/data.txt 2 "int,int"`
  2. `java -jar dist/simpledb.jar convert data/s.txt 3 "int,int,string"`
2. Every time you want to run an interactive session with the data saved, use the following command: `java -jar dist/simpledb.jar parser data/catalog.txt`

The data will be stored in `data/*.dat` files, though you should not need to worry about this.


### Exercise 1.1: Transactions

Transactions, as seen in class, can have multiple operations within them. For this exercise, you will execute the following queries in a single transaction:

1. List all name, age tuples in `s`.
2. List all elements in data.

Put these queries under "Your First Transaction" in the SQL file.

### Exercise 1.2: Update, Insert, Delete

While we have already written queries to read from the database, we have not explored writing to the database. Add the following operations to `queryFile.sql`, with each part as separate transactions:

1. Part 1:
  1. Insert these tuples into `data`: (6, 60), (10, 33).
  2. Insert these tuples into `s`: (6, 6, 'Michael'), (7, 60, 'Michelle').
2. Part 2:
  1. Update all entries in `data` that have an `f1` value of 1 to have an `f2` value of 1.
  2. Update all entries in `s` with the name "Michelle" to have an age of 100.
  3. Update all entries in`data` that have an `f2` value of 50 to have an `f2` value of 100.
3. Part 3:
  1. Delete all entries in `data` with an `f1` value of 2.
  2. Delete all entries in `s`.

At this point, you should be able to test if your implementation is correct by running the following command in your simpledb directory: `bash scripts/test.sh queryFile.sql`.


## Part 2: Rollback and Recovery

Now that you are familiar with .sql files, you can test some additional testing features that may be useful for debugging the remainder of this assignment. In particular, these commands should be relevant:

```
### flush;      // forces updates to disk
rollback;       // aborts an ongoing transaction and undos the changes
### checkpoint; // force a checkpoint in the log
### crash;      // crashes and restarts the database, then recovers
### printlog;   // prints the current state of the log record
```

We've provided a scaffold for you in `queryTest.sql`, which we won't grade - go ahead and write some SQL code to start a transaction, update a table, flush the updates to disk, and abort (instead of committing). Remember to print the table contents at the end, and run your implementation with `bash scripts/test.sh queryTest.sql`. You should observe that none of the updates are undone - by the end of this assignment, this should no longer be the case.

To provide some background, this assignment uses a steal, no-force buffer policy and page-based logging, which is possible because of page-level locking and a lack of indices (which may have different structures by UNDO time). Page-level locking allows for significant simplification at the cost of efficiency, as a transaction that modified a page must have had an exclusive lock on the page. No other transaction would concurrently modify this page, so changes can be undone by simply overwriting the whole page.  You do not have to worry about implementing this logging process, as it has already been done for you.

What you do need to know, among other things, is that log records for updates are written with a before-image and an after-image, containing the original contents of a page and the contents of the page after modification, respectively. You will use the before-image to roll back during aborts and to undo loser transactions during recovery, and the after-image to redo winners during recovery.

While you do not need to touch this file, BufferPool.java also contains code relevant to the logging process and is worth investigating. For example, does what you see agree with the steal, no-force policy?

### Exercise 2: LogFile.rollback()

Your first job is to enforce the atomicity property of database transactions. In particular, any transaction that aborts before committing and releasing its locks must have its changes to the database undone. With our scheme of page-level locking, we can overwrite the pages changed by aborted transactions with the before-images stored in the log file.

Implement the `rollback()` function in `LogFile.java`, which will be called when a transaction aborts. `rollback()` should read the log file, find all update records associated with the aborting transaction, extract the before-image from each, and write the before-image to the table file. (Previously, the instructions here said to replace any page in the buffer pool whose before-image you write back to the table file. You do not need to worry about this, as the buffer pool page will be removed and does not need to be undone.)

After completing this exercise, you should be able to pass the TestAbort and TestAbortCommitInterleaved subtests of the LogTest system test.

#### Tools & Hints:

- `tidToFirstLogRecord` maps transaction ids to an offset in the heap file, which can be used to determine where to start reading the log file for a particular transaction.

- The log file has a predefined format specified at the top of `LogFile.java`, and is implemented as a RandomAccessFile (http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html).
  - You should use `raf.seek()` to move around in the log file, and `raf.readInt()`, `raf.readLong()`, etc. to examine it.
  - Use `readPageData(raf)` to read each of the before- and after-images, which are stored immediately next to one another.

- As you develop your code, you may find the `LogFile.print()` method useful for displaying the current contents of the log. In fact, it might be a good idea to reference the code for printing, as it utilizes the same tools for accessing the log. 

- Likewise, you may be interested in looking at the functions `LogFile.java` has for generating and appending log records, such as `logAbort`, `logCommit`, and `logWrite`. Note that not all logs are the same length.

- After you are done with rollback, make sure that your `raf` file pointer is at the end of the last complete log record, `currentOffset`.


### Exercise 3: LogFile.recover()

Your second job is to enforce the durability property of database transactions. If the database crashes and reboots, it must recover before any new transactions start. In particular, your implementation should:

1. Read the last checkpoint, if any.
2. Scan forward from the checkpoint (or start of log file, if no checkpoint exists) to build the set of loser transactions. During this pass, updates should be redone.  Note that you can safely start REDO at the checkpoint because `LogFile.logCheckpoint()` flushes all dirty buffers to disk.
3. UNDO the updates of loser transactions.

Implement the `recover()` function in `LogFile.java`, which will be called on a database restart, before any new transactions start.

#### Hints:

- You may find the `logCheckpoint()` method useful for understanding how checkpoints are stored and how to find the latest checkpoint in the LogFile.

- When scanning forward, you will need to track all of the loser transactions that will need to be undone in the UNDO phase. Loser transactions are transactions that have not completed by the time you reach the end of the LogFile.

- After you are done with recovery, make sure that your `raf` file pointer is at the beginning of the last complete log record.
  
After completing this exercise, you should be able to pass the entirety of the LogTest system test. You can also write a simple test for recovery using `queryTest.sql`, using the `### crash;` command - can you figure out what, exactly, you want to test?


## Wrapping Up

### Testing
The following Ant commands will be useful:

    ant                             // compiles your code in src (defaults to ant dist)
    ant clean                       // deletes the /bin and /dist directory
    ant systemtest                  // run all system tests
    ant runsystest -Dtest=testname  // runs the tests in /test/simpledb/systemtest/testname.java
    
### Autograder & Assignment Submission

To submit your assignment, as before, push a branch containing the commit you want us to grade to release/hw5. **If you have a partner, make sure to push to the submission branch on Partner 1's repo.**

    $ git checkout -b release/hw5
    $ git push origin release/hw5

This process will also trigger the autograder. If your submission is successful, you should receive an email with the autograder results within an hour. If you do not receive a response after an hour, please first double-check that all your files are in the right place and that you pushed a commit to your release branch, and then notify us that you haven't received a response. Note that the autograder tests are the same as the ones we've provided for you, and are not comprehensive!

**Finally, remember that copying all or part of another person's work, and using reference material not specifically allowed, are forms of cheating and will not be tolerated.**

Good luck!


## Acknowledgements

This homework was adapted from past projects and labs in CS 186 and 6.830/6.814 at MIT.
