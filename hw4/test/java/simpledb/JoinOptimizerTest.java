package simpledb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class JoinOptimizerTest extends SimpleDbTestBase {

    /**
     * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create
     * an identical HeapFile table
     * 
     * @param tuples
     *            Tuples to create a HeapFile from
     * @param columns
     *            Each entry in tuples[] must have
     *            "columns == tuples.get(i).size()"
     * @param colPrefix
     *            String to prefix to the column names (the columns are named
     *            after their column number by default)
     * @return a new HeapFile containing the specified tuples
     * @throws IOException
     *             if a temporary file can't be created to hand to HeapFile to
     *             open and read its data
     */
    public static HeapFile createDuplicateHeapFile(
            ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix)
            throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
    }

    ArrayList<ArrayList<Integer>> tuples1;
    HeapFile f1;
    String tableName1;
    int tableId1;
    TableStats stats1;

    ArrayList<ArrayList<Integer>> tuples2;
    HeapFile f2;
    String tableName2;
    int tableId2;
    TableStats stats2;

    /**
     * Set up the test; create some initial tables to work with
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create some sample tables to work with
        this.tuples1 = new ArrayList<ArrayList<Integer>>();
        this.f1 = SystemTestUtil.createRandomHeapFile(10, 1000, 20, null,
                tuples1, "c");

        this.tableName1 = "TA";
        Database.getCatalog().addTable(f1, tableName1);
        this.tableId1 = Database.getCatalog().getTableId(tableName1);
        System.out.println("tableId1: " + tableId1);

        stats1 = new TableStats(tableId1, 19);
        TableStats.setTableStats(tableName1, stats1);

        this.tuples2 = new ArrayList<ArrayList<Integer>>();
        this.f2 = SystemTestUtil.createRandomHeapFile(10, 10000, 20, null,
                tuples2, "c");

        this.tableName2 = "TB";
        Database.getCatalog().addTable(f2, tableName2);
        this.tableId2 = Database.getCatalog().getTableId(tableName2);
        System.out.println("tableId2: " + tableId2);

        stats2 = new TableStats(tableId2, 19);

        TableStats.setTableStats(tableName2, stats2);
    }

    private double[] getRandomJoinCosts(JoinOptimizer jo, LogicalJoinNode js,
            int[] card1s, int[] card2s, double[] cost1s, double[] cost2s) {
        double[] ret = new double[card1s.length];
        for (int i = 0; i < card1s.length; ++i) {
            ret[i] = jo.estimateJoinCost(js, card1s[i], card2s[i], cost1s[i],
                    cost2s[i]);
            // assert that he join cost is no less than the total cost of
            // scanning two tables
            Assert.assertTrue(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }

    /**
     * Verify that the estimated join costs from estimateJoinCost() are
     * reasonable we check various order requirements for the output of
     * estimateJoinCost.
     */
    @Test
    public void estimateJoinCostTest() throws ParsingException {
        // It's hard to narrow these down much at all, because students
        // may have implemented custom join algorithms.
        // So, just make sure the orders of the return values make sense.

        TransactionId tid = new TransactionId();
        JoinOptimizer jo;
        Parser p = new Parser();
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName2
                + " t2 WHERE t1.c1 = t2.c2;"), new Vector<LogicalJoinNode>());
        // 1 join 2
        LogicalJoinNode equalsJoinNode = new LogicalJoinNode(tableName1,
                tableName2, Integer.toString(1), Integer.toString(2),
                Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 2 join 1
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName2
                + " t2 WHERE t1.c1 = t2.c2;"), new Vector<LogicalJoinNode>());
        equalsJoinNode = new LogicalJoinNode(tableName2, tableName1,
                Integer.toString(2), Integer.toString(1), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 1 join 1
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName1
                + " t2 WHERE t1.c3 = t2.c4;"), new Vector<LogicalJoinNode>());
        equalsJoinNode = new LogicalJoinNode(tableName1, tableName1,
                Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 2 join 2
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName2 + " t1, " + tableName2
                + " t2 WHERE t1.c8 = t2.c7;"), new Vector<LogicalJoinNode>());
        equalsJoinNode = new LogicalJoinNode(tableName2, tableName2,
                Integer.toString(8), Integer.toString(7), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
    }

    private void checkJoinEstimateCosts(JoinOptimizer jo,
            LogicalJoinNode equalsJoinNode) {
        int card1s[] = new int[20];
        int card2s[] = new int[card1s.length];
        double cost1s[] = new double[card1s.length];
        double cost2s[] = new double[card1s.length];
        Object[] ret;
        // card1s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 3 * i + 1;
            card2s[i] = 5;
            cost1s[i] = cost2s[i] = 5.0;
        }
        double stats[] = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s,
                cost1s, cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // card2s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 4;
            card2s[i] = 3 * i + 1;
            cost1s[i] = cost2s[i] = 5.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // cost1s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = card2s[i] = 7;
            cost1s[i] = 5.0 * (i + 1);
            cost2s[i] = 3.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // cost2s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = card2s[i] = 9;
            cost1s[i] = 5.0;
            cost2s[i] = 3.0 * (i + 1);
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // everything linear
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 2 * (i + 1);
            card2s[i] = 9 * i + 1;
            cost1s[i] = 5.0 * i + 2;
            cost2s[i] = 3.0 * i + 1;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkQuadratic(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
    }

    /**
     * Verify that the join cardinalities produced by estimateJoinCardinality()
     * are reasonable
     */
    @Test
    public void estimateJoinCardinality() throws ParsingException {
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
        JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid,
                "SELECT * FROM " + tableName2 + " t1, " + tableName2
                        + " t2 WHERE t1.c8 = t2.c7;"),
                new Vector<LogicalJoinNode>());

        double cardinality;

        cardinality = j.estimateJoinCardinality(new
        LogicalJoinNode(tableName1, tableName2, Integer.toString(3),
        Integer.toString(4), Predicate.Op.EQUALS),
        stats1.estimateTableCardinality(0.8),
        stats2.estimateTableCardinality(0.2), false, false, TableStats.getStatsMap());

        // We don't specify in what way statistics should be used to improve
        // these estimates. So, just require that they not be entirely
        // unreasonable.
        Assert.assertTrue(cardinality > 800);
        Assert.assertTrue(cardinality <= 2000);

        cardinality = j.estimateJoinCardinality(new
        LogicalJoinNode(tableName2, tableName1, Integer.toString(3),
        Integer.toString(4), Predicate.Op.EQUALS),
        stats2.estimateTableCardinality(0.2),
        stats1.estimateTableCardinality(0.8), false, false, TableStats.getStatsMap());

        Assert.assertTrue(cardinality > 800); Assert.assertTrue(cardinality
        <= 2000);

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + Integer.toString(3), "c" + Integer.toString(4),
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.8),
                stats2.estimateTableCardinality(0.2), true, false, TableStats
                        .getStatsMap());

        // On a primary key, the cardinality is well-defined and exact (should
        // be size of fk table)
        // BUT we had a bug in lab 4 in 2009 that suggested should be size of pk
        // table, so accept either
        Assert.assertTrue(cardinality == 800 || cardinality == 2000);

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + Integer.toString(3), "c" + Integer.toString(4),
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.8),
                stats2.estimateTableCardinality(0.2), false, true, TableStats
                        .getStatsMap());

        Assert.assertTrue(cardinality == 800 || cardinality == 2000);
    }

}
