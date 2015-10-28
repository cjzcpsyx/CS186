SimpleDB: Query Optimizer
=========================

This assignment is due **Wednesday, November 4th at 11:59 PM** and is worth **10% of your final grade**.

Project Description
-------------------

In this project, you will implement a query optimizer on top of SimpleDB. The main tasks include implementing a
selectivity estimation framework and a cost-based optimizer based on the Selinger cost-based optimizer discussed in class. The remainder of this document describes what is involved in adding optimizer support and provides a basic outline of how you can add this support to your database. As with the previous projects, we recommend that you start as early as possible.

## 0. A few notes
- SimpleDB is, unfortunately, not simple. A big part of this project is to be able to read javadoc and source code, and to be able to understand not only the parts you need to implement, but also already implemented classes and methods.
  - However, **PLEASE DO NOT** change any of the interfaces and classes given unless we specify for you to do so! If you do change an external file, then you may pass the local tests, but **you will fail** our autograder!
- If there's anything that doesn't make sense (anything, from tests cases to method descriptions), try to first *guess*
what's going on, and if things still don't make sense, post what you think is happening and what's going wrong on
Piazza. Tag your questions like "[HW4 P1]" for this homework's "Exercise 1"
- Setup will be better with this assignment, because we don't have to deal with vagrant (thank the system lords).
- Check Piazza for bugs and typos. We'll update the git repo if there are mistakes on our end and note that we have
on Piazza. A lot of this project is freshly baked so please be patient.

## 1. Getting started 

Since we are working with SimpleDB, we will be reusing the environment setup from HW2 (Java 1.7+). However, we'll be 
using a fresh version of SimpleDB, so just pull the code from the repo and start working!

### 1.1 Implementation hints

We suggest exercises along this document to guide your implementation, but you may find that a different order makes
more sense for you.

Here's a rough outline of one way you might proceed with this project:

- Implement the methods in the `TableStats` class that allow it to estimate selectivities of filters and cost of scans. We estimate selectivities using the assumption that our values are uniformly distributed (skeleton provided for the `IntStatistics` class).

- Implement the methods in the `JoinOptimizer` class that allow it to estimate the cost and selectivities of joins.

- Write `orderDynamicProgrammingJoins` and `orderGreedyJoins` method in `JoinOptimizer`. This method must produce a 
(close to) optimal ordering for a series of joins, given statistics computed in the previous two steps.

## 2. Optimizer outline
Recall that the main idea of a cost-based optimizer is to:

- Use statistics about tables to estimate "costs" of different query plans. Typically, the cost of a plan is related 
to the cardinalities of (number of tuples produced by) intermediate joins and selections, as well as the selectivity of
filter and join predicates.
- Use these statistics to order joins and selections in an optimal way, and to select the best implementation for join
 algorithms from amongst several alternatives.

In this project, you will implement code to perform both of these functions.

The optimizer will be invoked from `simpledb/Parser.java`. When the Parser is invoked, it will compute statistics over all
of the tables (using statistics code you provide). When a query is issued, the parser will convert the query into a
logical plan representation and then call your query optimizer to generate an optimal plan.

### 2.1 Overall Optimizer Structure
    
Before getting started with the implementation, you need to understand the overall structure of the SimpleDB optimizer.
The overall control flow of the SimpleDB modules of the parser and optimizer is shown in below.
![Image](http://i59.tinypic.com/26382fc.png?height=100)

*Figure 1: Diagram illustrating classes, methods, and objects used in the parser and optimizer.*

The key at the bottom explains the symbols; you will implement the components with double-borders. The classes and
methods will be explained in more detail in the text that follows (you may wish to refer back to this diagram), but the
basic operation is as follows:

- `Parser.java` constructs a set of table statistics (stored in the `statsMap` container) when it is initialized. It then 
waits for a query to be input, and calls the method parseQuery on that query.
- `parseQuery` first constructs a LogicalPlan that represents the parsed query. `parseQuery` then calls the method 
`physicalPlan` on the `LogicalPlan` instance it has constructed. The `physicalPlan` method returns a DBIterator object
that can be used to actually run the query.

In the exercises to come, you will implement the methods that help `physicalPlan` devise an optimal plan.

### 2.2 Statistics Estimation

Accurately estimating plan cost is quite tricky. In this project, we will focus only on the cost of sequences of joins
and base table accesses.  We won't worry about access method selection (since we only have one access method, table
scans) or the costs of additional operators (like aggregates). You are only required to consider left-deep plans for
this project although we provide methods that will help you search through a larger variety of plans (the set
of all linear plans).

#### 2.2.1 Overall Plan Cost
We will write join plans of the form `p = t1 join t2 join ... tn`, which signifies a left deep join where t1 is the
left-most join (deepest in the tree). Given a plan like `p`, its cost can be expressed as:

    iocost(t1) + iocost(t2) + cpucost(t1 join t2) +
    iocost(t3) + cpucost((t1 join t2) join t3) +

Here, `iocost(t1)` is the I/O cost of scanning table `t1`, `cpucost(t1 join t2)` is the CPU cost to join `t1` to `t2`.

#### 2.2.2 Join Cost
When using nested loops joins, recall that the cost of a join between two tables `t1` and `t2` (where `t1` is the 
outer) is simply:

    joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost
                           + ntups(t1) x ntups(t2)  //CPU cost

Here, `ntups(t1)` is the number of tuples in table `t1`.

#### 2.2.3 Filter Selectivity
The value of `ntups` can be directly computed for a base table by scanning that table. Estimating `ntups` for a table
with one or more selection predicates over it can be trickier -- this is the filter selectivity estimation problem.
The simplest approach to estimate selectivity is to assume that our values are uniformly distributed and that our terms are independent. This is the approach we will take for this project. However, another common method uses histograms, which can be seen in `StringHistogram`. 

To estimate the selectivity of a predicate, refer to the following formulas. These formulas assume a uniform, 
 independent distribution, and are derived from basic probability.
  - `col = value` : `RF = 1/NDistinct(T)`
  - `col1 = col2` : `RF = 1/MAX(NDistinct(T1), NDistinct(T2))`
  - `col > value` : `RF = (High(T)-value)/(High(T)-Low(T))`

One important question is how to calculate `NDistinct(T)`. One possibility is using a set, but that takes `O(n)` memory in the worse case, which we cannot afford. Thus, we've provided one solution, which is to use a constant size hash map to *approximate* the number of distinct tuples (feel free to peruse `IntStatistics` if you're interested). But since this is an approximation, `numDistinct` that we've calculated [here](https://github.com/berkeley-cs186/course/blob/master/hw4/src/java/simpledb/IntStatistics.java#L59) will not be exact.

### Exercise 1: IntStatistics.java

You will need to implement some way to record table statistics for selectivity estimation. We have provided a skeleton
class, `IntStatistics` that will do this. 

We have provided a class `StringHistogram` that computes selectivities for String predicates. You 
may modify `StringHistogram` if you want to implement a better estimator, although it's not necessary for completing 
this project.

After completing this exercise, you should be able to pass the `IntStatisticsTest` unit test (`ant runtest 
-Dtest=IntStatisticsTest`).

#### Classes to know about:
- `Predicate.Op`: contains all the types of predicates we may have, such as "EQUALS", "GREATER_THAN", and so on.

#### Implementation hints:
- Don't fret if you're failing some tests, careful debugging will save you here. Be familiar with what 
`IntStatisticsTest.java` contains!

### Exercise 2: TableStats.java

The class `TableStats` contains methods that compute the number of tuples and pages in a table and that estimate the
selectivity of predicates over ALL the fields of that table. The query parser we have created creates one instance of
`TableStats` per table, and passes these structures into your query optimizer (which you will need in later exercises).

You should fill in the following methods and classes in `TableStats`:

- Implement the `TableStats` constructor: Once you have implemented a method for tracking statistics such as histograms,
you should implement the `TableStats` constructor, adding code to scan the table to build the statistics you need. 
The flow of the constructor should look like the following:
  - Create a histogram or statistic for every attribute in the table. If the attribute is a string attribute, create 
  a `StringHistogram`. If it is an integer, create a `IntStatistics`. 
  - Scan the table, selecting out all of the fields of all of the tuples and use them to populate the corresponding 
  statistic or histogram.
- Implement `estimateSelectivity (int field, Predicate.Op op, Field constant)`: Using your statistics (e.g., an 
`IntStatistics` or `StringHistogram` depending on the type of the field - SimpleDB only has integers or string field 
types!), estimate the selectivity of predicate `field op constant` on the table.
- Implement `estimateScanCost()`: This method estimates the cost of sequentially scanning the file, given that the 
cost to read a page is `costPerPageIO`. You can assume that there are no seeks and that no pages are in the buffer pool.
This method may use costs or sizes you computed in the constructor.
- Implement `estimateTableCardinality(double selectivityFactor)`: This method returns the number of tuples in the 
relation, given that a predicate with selectivity `selectivityFactor` is applied. This method may use costs or sizes
you computed in the constructor.

After completing these tasks you should be able to pass the unit tests in `TableStatsTest` (`ant runtest 
-Dtest=TableStatsTest`).

#### Classes to know about:
- `TupleDesc`: the class that represents the schema of a table (all the field names and their types of a table).
- `Type`: in particular `Type.INT_TYPE` and `Type.STRING_TYPE` can help you here.
- `IntStatistics` and `StringHistogram`: you'll need them!
- `DbFileIterator`: iterates over tuples in a table.

#### Implementation hints:
- If you think your implementation for `estimateScanCost` and `estimateTableCardinality` is too simple, don't worry, 
you're implementation is (probably) right!
- If you're failing `TableStatsTests`, it is possible that your `IntStatistics` needs more fixing :/

### 2.2.4 Join Cardinality
Finally, observe that the cost for the join plan `p` above includes expressions of the form `joincost((t1 join t2) 
join t3)`. To evaluate this expression, you need some way to estimate the size (`ntups`) of `t1 join t2`. This join
cardinality estimation problem is harder than the filter selectivity estimation problem. In this project, you aren't
required to do anything fancy for this. While implementing your simple solution, you should keep in mind the following:
    
- For equality joins, when one of the attributes is a primary key, the number of tuples produced by the join cannot be
larger than the cardinality of the non-primary key attribute. 
- For equality joins when there is no primary key, it's hard to say much about what the size of the output is - it 
could be the size of the product of the cardinalities of the tables (if both tables have the same value for all tuples)
or it could be `0`. For this project, we'll use a simple heuristic: the size of the larger of the two tables.
- For range scans, it is similarly hard to say anything accurate about sizes. The size of the output should be 
proportional to the sizes of the inputs. It is fine to assume that a fixed fraction, *30%*, of the cross-product is 
emitted by range scans. In general, the cost of a range join should be larger than the cost of a non-primary 
key equality join of two tables of the same size.

### Exercise 3: Join Cost Estimation
    
The class `JoinOptimizer.java` includes all of the methods for ordering and computing costs of joins. In this exercise,
you will write the methods for estimating the selectivity and cost of a join, specifically:

- Implement `estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2)`: This method 
estimates the cost of join `j`, given that the left input is of cardinality `card1`, the right input of cardinality
`card2`, that the cost to access the left input is `cost1`, and that the cost to access the right input is `cost2`.
You can assume we're using a nested loops join, and apply the formula in section *2.2.2 Join Cost*.
- Finish implementing `estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean 
t2pkey)`: This method estimates the number of tuples output by join `j`, given that the left input is size `card1`, 
the right input is size `card2`, and the flags `t1pkey` and `t2pkey` that indicate whether the left and right 
(respectively) field is unique (a primary key). To complete it, implement `estimateTableJoinCardinality`.

After implementing these methods, you should be able to pass the unit tests in `JoinOptimizerTest.java` (`ant runtest
 -Dtest=JoinOptimizerTest`).
 
#### Classes to know about:
- `LogicalJoinNode`: actually, you *don't* really need to know this for this exercise. You'll see it in the next one ;)

#### Implementation hints:
- Read sections *2.2.2 Join Cost* and *2.2.4 Join Cardinality* to understand the specs here! The methods aren't 
complicated, you can pass the tests if you just follow what they say.
- You may not need to use all variables passed into the methods - they are there if you want to make a more 
sophisticated estimation.

### 2.3 Join Ordering

Now that you have implemented methods for estimating costs, you will implement a Selinger-style optimizer. For these
methods, joins are expressed as a list of join nodes (e.g., predicates over two tables) as opposed to a list of
relations to join as described in class.

### 2.3.1 The Dynamic Programming Algorithm
Translating the algorithm to the join node list form mentioned above, an outline in pseudocode would be as follows:

    1. j = set of join nodes
    2. for (i in 1...|j|):  // First find best plan for single join, then for two joins, etc. 
    3.     for s in {all length i subsets of j} // Looking at a concrete subset of joins
    4.       bestPlan = {}  // We want to find the best plan for this concrete subset 
    5.       for s' in {all length i-1 subsets of s} 
    6.            subplan = optjoin(s')  // Look-up in the cache the best query plan for s but with one relation missing
    7.            plan = best way to join (s-s') to subplan // Now find the best plan to extend s' by one join to get s
    8.            if (cost(plan) < cost(bestPlan))
    9.               bestPlan = plan // Update the best plan for computing s
    10.      optjoin(s) = bestPlan
    11. return optjoin(j)

To help you implement this algorithm, we have provided several classes and methods to assist you. First, the method
`enumerateSubsets(Vector v, int size)` in `JoinOptimizer.java` will return a set of all of the subsets of `v` of size 
`size`. This method is not particularly efficient; you can try to implement a more efficient enumerator by yourself, but
it's not necessary for this project.

Second, we have provided the method:

    private CostCard computeCostAndCardOfSubplan(HashMap<String, TableStats> stats, 
                                                HashMap<String, Double> filterSelectivities, 
                                                LogicalJoinNode joinToRemove,  
                                                Set<LogicalJoinNode> joinSet,
                                                double bestCostSoFar,
                                                PlanCache pc)

Given a subset of joins (`joinSet`), and a join to remove from this set (`joinToRemove`), this method computes the best
way to join `joinToRemove` to `joinSet - {joinToRemove}`. It returns this best method in a `CostCard` object, which
includes the cost, cardinality, and best join ordering (as a vector). `computeCostAndCardOfSubplan` may return `null`,
if no plan can be found (because, for example, there is no linear join that is possible), or if the cost of all plans
is greater than the `bestCostSoFar` argument. The method uses a cache of previous joins called `pc` (`optjoin` in the 
pseudocode above) to quickly lookup the fastest way to join `joinSet - {joinToRemove}`. The other arguments (`stats`
and `filterSelectivities`) are passed into the `orderDynamicProgrammingJoins` method that you must implement as a part of Exercise 4, 
and are explained below. This method essentially performs lines 6-8 of the psuedocode described earlier.

Third, we have provided a class `PlanCache` that can be used to cache the best way to join a subset of the joins 
considered so far in your implementation of the Selinger-style optimizer (an instance of this class is needed to use 
`computeCostAndCardOfSubplan`).

### Exercise 4: Join Ordering

In `JoinOptimizer.java`, implement the method:

    Vector orderDynamicProgrammingJoins(HashMap<String, TableStats> stats, 
                                        HashMap<String, Double> filterSelectivities)

This method should operate on the `joins` class member, returning a new `Vector` that specifies the order in which 
joins should be done. Item `0` of this vector indicates the bottom-most join in a linear plan. Adjacent joins in the 
returned vector should share at least one field to ensure the plan is linear. Here stats is an object that lets you
find the `TableStats` for a given table name that appears in the `FROM` list of the query. `filterSelectivities` allows
you to find the selectivity of any predicates over a table; it is guaranteed to have one entry per table name in the
`FROM` list. Finally, explain specifies that you should output a representation of the join order for informational
purposes.

You may wish to use the helper methods and classes described above to assist in your implementation. Roughly, your
implementation should follow the pseudocode above, looping through subset sizes, subsets, and sub-plans of subsets,
calling `computeCostAndCardOfSubplan` and building a `PlanCache` object that stores the minimal-cost way to perform
each subset join.

After implementing `orderDynamicProgrammingJoins`, you should be able to pass the test `DpJoinTest`.

#### Classes to know about:
- `LogicalJoinNode`: remember, this represents a single join of two tables.
- `CostCard`: a POJO that just helps store `cost`, `cardinality`, and `plan` objects in one place.
- `JoinOptimizer`: really keep the methods listed above in mind to implement this. Read the javadocs for:
  - `enumerateSubsets`
  - `computeCostAndCardOfSubplan`

#### Implementation hints:
- The result that you return, `Vector<LogicalJoinNode>` represents the ORDER in which you execute your joins. The 
first node is the first two tables you'll be joining, and so on.
- The test cases are very loose, make sure to visually inspect your join (see the systemtest) and hand check if it
makes sense! The system test by itself **DOES NOT CHECK CORRECTNESS**. That's up to you to hand check.

### 2.3.2 The Greedy Algorithm
Remember that the dynamic programing algorithm can be exponential in time and space, so after we start
joining too many tables, it becomes infeasible. Thus, we have the polynomial time greedy algorithm! Here is the idea:

    1. plan <- [] // our proposed plan starts out empty
    2. joinsLeft <- all the joins // joinsLeft represents which joins we haven't added to the plan yet
    3. while ( |joinsLeft| > 0 ) { // continue until we've processed all joins
    4.     cheapestJoin <- minimial cost join of all joins in joinsLeft
    5      add cheapestJoin to our plan
    6.     remove cheapestJoin from joinsLeft
    7.     update whatever cardinality / costs computations we've kept track of so far
    8. return plan
    
The idea in the greedy algorithm is that at each step, we choose the cheapest possible join to make, and make it!

### Exercise 5: Greedy Join Ordering
After implementing `orderGreedyJoins`, you should be able to pass the test `GreedyJoinTest`.

#### Classes to know about:
- Not much anymore. Thank goodness

#### Implementation hints:
- You'll find the method `costGreedyJoin` helpful.
- Like the dynamic programming algorithm, we need to keep track of the intermediate join values, their costs and 
cardinalities. That's what `planCardinalities` and `planCosts` are for!

### 2.2.3 Do you want to see a picture?
SimpleDB can visualize the join ordering that your algorithm produces. To see yours, run `ant systemtest`. This is
NOT a JUnit test, it is just to help you visualize what your join order and costs look like. However, make sure
that the test doesn't fail, since that indicates that your join ordering algorithm(s) are too slow.

Are your orderings correct? Look through `QueryTest.java` and decide for yourselves!

## Wrapping Up

### Testing
The following Ant commands will be useful:

    ant                             // compiles your code in src (defaults to ant dist)
    ant clean                       // deletes the /bin and /dist directory
    ant test                        // run all unit tests
    ant systemtest                  // run all integration/system tests
    ant runtest -Dtest=testname     // runs the test in /test/testnameTest.java
    
### Autograder

To run the autograder on your assignment, you must push to your submission branch: release/hw4. **If you have a partner, make sure to push to the submission branch on partner 1's repo.** This process ensures that you're
submitting the assignment correctly in addition to getting an autograder email. 

    $ git checkout -b release/hw4
    $ git push origin release/hw4

Our machines will e-mail you the results of the autograder within an hour. If you do not receive a response after an
hour, please first double-check that all your files are in the right place and that you pushed a commit to your
release branch, and then notify us that you haven't received a response. Note that these tests are the same as the
ones we've provided for you and are not comprehensive!

### Assignment Submission
To submit your assignment, as before, push a branch containing the commit you want us to grade to release/hw4. This
process is the same as triggering the autograder above. **Finally, remember that copying all or part of another
person's work, or using reference material not specifically allowed, are forms of cheating and will not be tolerated.**
