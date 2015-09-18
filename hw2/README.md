# Homework 2: Query Plans and Joins

This assignment is due **Monday, September 28th at 11:59 PM** and is worth **8% of your final grade**.

Now that you've written some SQL queries of your own in HW1, let's dive deeper into what databases do to make those SQL queries happen! You're going to be using SimpleDB, a basic database management system that is written in Java, to implement this homework assignment. Specifically, you will be writing query plans and implementing joins through SimpleDB. 

# Getting Started

## Partners

This homework is an excellent opportunity to collaborate, but if you want to work alone, that's cool too!
Before you start coding, fill out this [Google Form](https://docs.google.com/a/berkeley.edu/forms/d/1y0Oc9T7jHal2cEeSOv4St91r3CsDS62MSGy-2dwQYHM/viewform) to have your team registered with course staff.  **Only one partner should fill out the form**, and, likewise, only one partner will be submitting through their Github repository.

## Setup

SimpleDB will need the Java Development Kit and the Ant build tool to compile code and run tests.  Ant is similar to make, but the build file is written in XML and is somewhat better suited to Java code. Most modern Linux distributions include Ant.  Our recommendations for installing JDK and Ant are as follows:

### OS X Users

1. Download and install the [Java Development Kit](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (select the correct file under "Java SE Development Kit 8u60").
2. Download and install [Homebrew](http://brew.sh/), and run `brew update`.
3. Install ant with `brew install ant`.

### Windows Users

1. Download and install the [Java Development Kit](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (select the correct file under "Java SE Development Kit 8u60").  Make a note of your install location - you will need it later.
2. Download and install [WinAnt](https://code.google.com/p/winant/).  This is a tool that will set up ant for you, which is much simpler than the [alternative](http://www.mkyong.com/ant/how-to-install-apache-ant-on-windows/).

### Linux Users

If JDK and/or Ant are not already installed, run:

    $ sudo apt-get update
    $ sudo apt-get install openjdk-8-jdk
    $ sudo apt-get install ant

# Making Query Plans

## The Filter Operator

Operators are used in SQL to specify conditions and conjoin multiple tables. They are used primarily in the WHERE clause to perform arithmetic, comparison, and logical operations. In particular, the Filter operator performs a relational select by applying a predicate to a single table.

### Task #1: Implementing Filter

To start off, let's implement the Filter operator. After completing the methods in `Filter.java`, you should be passing the tests in `FilterTest.java`. You can run these tests with the command

    $ ant runtest -Dtest=FilterTest

Tip: You may want to look at other classes that extend Operator to get a feel for how to implement this operator.

## Query Plans

In lecture, you briefly learned about conceptualizing SQL evaluations through query execution plans. Any given database management system will have a query parser and optimizer that takes in a query and returns a query plan that will be executed. For this project, you will play the role of the query parser! You will be learning about query optimization later on in the semester, so for this project you do not need to worry about optimizing your query plan.

### Task #2: Crafting Your Own Query Plans

For this task, develop and write a query plan in `QueryPlans.java` for each of the following queries using the operators given to you (including Filter):

#### Query 1

    SELECT *
    FROM T1, T2
    WHERE T1.column0 = T2.column0;
    
#### Query 2

    SELECT * 
    FROM T1, T2
    WHERE T1.column0 > 1
      AND T1.column1 = T2.column1;
      
#### Query 3

    SELECT column0, MAX(column1)
    FROM T1
    WHERE column2 > 1
    GROUP BY column0;
    
#### Query 4

    SELECT *
    FROM T1, T2
    WHERE T1.column0 < (SELECT COUNT(*) FROM T3)
      AND T2.column0 = (SELECT AVG(column0) FROM T3)
      AND T1.column1 >= T2. column1
    ORDER BY T1.column0 DESC;

At this point, you should be passing the tests in `QueriesTest.java`. You can run these tests with the command

    $ ant runtest -Dtest=QueriesTest

# Implementing Joins

## Chunk Nested Loop Join

The first join you will be implementing is the Chunk Nested Loop Join, the same Chunk Nested Loop Join you learned in lecture and discussion! Although the general algorithm remains the same, there are a few changes you need to make due to the nature and scope of this homework. First, tables are handled as Iterators and tuples are accessed one by one; thus, you'll only be working with whole tables and their tuples. In this algorithm, you load a chunk with as many tuples as it can hold from one table; in our implementation, we will use an arbitrary `chunkSize`. Then, as you iterate through the chunk, check if there are any matches in the other table. If there are any, join them together as one tuple and return the newly concatenated tuple.

### Task #3: Implementing `ChunkNestedLoopJoin`

We have provided skeleton code for your Chunk Nested Loop Join implementation in `Chunk.java` and `ChunkNestedLoopJoin.java`.  In particular, you will need to fill in the methods of `Chunk.java` and implement `getCurrentChunk`, `fetchNextChunk`, and `fetchNext` in `ChunkNestedLoopJoin.java`, along with `open` and `close`. You *must* use `Chunk.java` to load and reference your tuples in your join implementation. Do not change any existing methods or method signatures.

At this point, you should be passing the tests in `ChunkNestedLoopJoinTest.java`. You can run these tests with the command

    $ ant runtest -Dtest=ChunkNestedLoopJoinTest

Tip: If you find yourself stuck, you may find `Join.java` to be a useful reference.

## Symmetric Hash Join

[Symmetric (or "pipelining") hash join](https://cs.uwaterloo.ca/~david/cs448/symmetric-hash-join.pdf) 
is a join algorithm that was designed for one of the original
parallel query engines, and makes effective use of streaming (non-blocking) behaviors.
In this algorithm, you construct a hash table for _both_ sides of the join, not just
one side as in regular hash join.  Having hashtables on both sides means that tuples can arrive
from either input in any order, and they can be handled immediately (non-blocking!) to produce matches and be hashed for later lookups from the other input.

We will be implementing a version of symmetric hash join that works as follows:
We'll begin by considering the "left" table as the inner table of the join, and the "right" table as the outer.
We begin by streaming tuples in from the inner relation. For every tuple we stream in
from the inner relation, we insert it into the inner hash table. We then check if
there are any matching tuples in the hashtable for the other relation -- if there are, then we join this
tuple with the corresponding matches. Otherwise, we switch the inner
and outer relations -- that is, the old inner becomes the new outer and the old outer
becomes the new inner, and we proceed to repeat this algorithm, streaming from the
new inner.

### Task #4: Implementing `SymmetricHashJoin`

We have provided skeleton code for your Symmetric Hash Join implementation in `SymmetricHashJoin.java`.  For this task, you will need to implement the `fetchNext` and `switchRelations` methods (along with `open` and `close`) in `SymmetricHashJoin.java`.  Implementing helper functions is probably a good idea, particularly for `fetchNext` (note that your helper methods must specify the same two exceptions that `fetchNext` can throw).  Do not change any existing methods or method signatures.

At this point, you should be passing the tests in `SymmetricHashJoinTest.java`.  You can run these tests with the command

    $ ant runtest -Dtest=SymmetricHashJoinTest

# Wrapping Up

## Testing

The following Ant commands will be useful:

    $ ant                             // compiles your code in src (defaults to ant dist)
    $ ant clean                       // deletes the /bin and /dist directory
    $ ant test                        // run all tests
    $ ant runtest -Dtest=testname     // runs the test in /test/testnameTest.java

We encourage you to write your own tests, as we have provided you with only a subset of what we will be grading with.  To add new test files, add `*Test.java` to the `test/simpledb` folder, where `*` is the test name of your choice.

## Autograder

We will set up an autograder for you to check your submissions.  More to come soon!

## Submission

To submit your assignment, push a branch containing the commit you want us to grade to `release/hw2`.  If you are working with a partner, we will only grade the submission in Partner 1's repository, as designated on the Google Form.

    $ git add . // or git add <file>
    $ git commit -m "<message>"
    $ git push origin master:release/hw2

Good luck!

This homework was adapted from past projects and labs in CS 186 and 6.830/6.814 at MIT.
