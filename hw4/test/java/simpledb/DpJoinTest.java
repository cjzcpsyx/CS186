package simpledb;

import junit.framework.Assert;
import org.junit.Test;
import simpledb.systemtest.SystemTestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

public class DpJoinTest {

    /**
     * Determine whether the orderDynamicProgrammingJoins implementation is doing a reasonable job
     * of ordering joins, and not taking an unreasonable amount of time to do so
     */
    @Test
    public void orderJoinsTest() throws ParsingException, IOException,
            DbException, TransactionAbortedException {
        // This test is intended to approximate the join described in the
        // "Query Planning" section of 2009 Quiz 1,
        // though with some minor variation due to limitations in simpledb
        // and to only test your integer-heuristic code rather than
        // string-heuristic code.

        final int IO_COST = 101;

        // Create a whole bunch of variables that we're going to use
        TransactionId tid = new TransactionId();
        JoinOptimizer j;
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();

        // Create all of the tables, and add them to the catalog
        ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null,
                empTuples, "c");
        Database.getCatalog().addTable(emp, "emp");

        ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null,
                deptTuples, "c");
        Database.getCatalog().addTable(dept, "dept");

        ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null,
                hobbyTuples, "c");
        Database.getCatalog().addTable(hobby, "hobby");

        ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null,
                hobbiesTuples, "c");
        Database.getCatalog().addTable(hobbies, "hobbies");

        // Get TableStats objects for each of the tables that we just generated.
        stats.put("emp", new TableStats(
                Database.getCatalog().getTableId("emp"), IO_COST));
        stats.put("dept",
                new TableStats(Database.getCatalog().getTableId("dept"),
                        IO_COST));
        stats.put("hobby",
                new TableStats(Database.getCatalog().getTableId("hobby"),
                        IO_COST));
        stats.put("hobbies",
                new TableStats(Database.getCatalog().getTableId("hobbies"),
                        IO_COST));

        // Note that your code shouldn't re-compute selectivities.
        // If you get statistics numbers, even if they're wrong (which they are
        // here
        // because the data is random), you should use the numbers that you are
        // given.
        // Re-computing them at runtime is generally too expensive for complex
        // queries.
        filterSelectivities.put("emp", 0.1);
        filterSelectivities.put("dept", 1.0);
        filterSelectivities.put("hobby", 1.0);
        filterSelectivities.put("hobbies", 1.0);

        // Note that there's no particular guarantee that the LogicalJoinNode's
        // will be in
        // the same order as they were written in the query.
        // They just have to be in an order that uses the same operators and
        // semantically means the same thing.
        nodes.add(new LogicalJoinNode("hobbies", "hobby", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "dept", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "hobbies", "c2", "c0",
                Predicate.Op.EQUALS));
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;"),
                nodes);

        // Order the joins!
        result = j.orderDynamicProgrammingJoins(stats, filterSelectivities);

        // There are only three join nodes; if you're only re-ordering the join
        // nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // There were a number of ways to do the query in this quiz, reasonably
        // well;
        // we're just doing a heuristics-based optimizer, so, only ignore the
        // really
        // bad case where "hobbies" is the outermost node in the left-deep tree.
        Assert.assertFalse(result.get(0).t1Alias == "hobbies");

        // Also check for some of the other silly cases, like forcing a cross
        // join by
        // "hobbies" only being at the two extremes, or "hobbies" being the
        // outermost table.
        Assert.assertFalse(result.get(2).t2Alias == "hobbies"
                && (result.get(0).t1Alias == "hobbies" || result.get(0).t2Alias == "hobbies"));
    }

    /**
     * Test a much-larger join ordering, to confirm that it executes in a reasonable amount of time
     */
    @Test(timeout=60000) public void bigOrderJoinsTest() throws IOException, DbException, TransactionAbortedException, ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        HashMap<String, TableStats> stats = new HashMap<String,TableStats>();
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100, Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileC = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileD = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileE = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileF = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileG = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileH = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileI = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileJ = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileK = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileL = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileM = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
        HeapFile smallHeapFileN = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples, 2, "c");

        ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        for (int i = 0; i < 100000; i++) {
            bigHeapFileTuples.add( smallHeapFileTuples.get( i%100 ) );
        }
        HeapFile bigHeapFile = JoinOptimizerTest.createDuplicateHeapFile(bigHeapFileTuples, 2, "c");
        Database.getCatalog().addTable(bigHeapFile, "bigTable");

        // Add the tables to the database
        Database.getCatalog().addTable(bigHeapFile, "bigTable");
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");
        Database.getCatalog().addTable(smallHeapFileJ, "j");
        Database.getCatalog().addTable(smallHeapFileK, "k");
        Database.getCatalog().addTable(smallHeapFileL, "l");
        Database.getCatalog().addTable(smallHeapFileM, "m");
        Database.getCatalog().addTable(smallHeapFileN, "n");

        // Come up with join statistics for the tables
        stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("j", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("k", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("l", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("m", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("n", new TableStats(smallHeapFileG.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("bigTable", 1.0);
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);
        filterSelectivities.put("j", 1.0);
        filterSelectivities.put("k", 1.0);
        filterSelectivities.put("l", 1.0);
        filterSelectivities.put("m", 1.0);
        filterSelectivities.put("n", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("i", "j", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("j", "k", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("k", "l", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("l", "m", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("m", "n", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("n", "bigTable", "c0", "c0", Predicate.Op.EQUALS));

        // Make sure we don't give the nodes to the optimizer in a nice order
        Collections.shuffle(nodes);
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(tid, "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;"),
                nodes);

        // Set the last boolean here to 'true' in order to have orderDynamicProgramming() print out its logic
        result = j.orderDynamicProgrammingJoins(stats, filterSelectivities);

        // If you're only re-ordering the join nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // Make sure that "bigTable" is the outermost table in the join
        Assert.assertEquals(result.get(result.size()-1).t2Alias, "bigTable");
    }

    /**
     * Test a join ordering with an inequality, to make sure the inequality gets
     * put as the innermost join
     */
    @Test
    public void nonequalityOrderJoinsTest() throws IOException, DbException,
            TransactionAbortedException, ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100,
                Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileC = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileD = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileE = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileF = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileG = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileH = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileI = JoinOptimizerTest.createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");

        // Add the tables to the database
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");

        // Come up with join statistics for the tables
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileH.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileI.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1",
                Predicate.Op.LESS_THAN));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));

        Parser p = new Parser();
        // Run the optimizer; see what results we get back
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT COUNT(a.c0) FROM a, b, c, d,e,f,g,h,i WHERE a.c1 < b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0;"),
                nodes);

        result = j.orderDynamicProgrammingJoins(stats, filterSelectivities);

        // If you're only re-ordering the join nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // Make sure that "a" is the outermost table in the join
        Assert.assertTrue(result.get(result.size() - 1).t2Alias.equals("a")
                || result.get(result.size() - 1).t1Alias.equals("a"));
    }
}
