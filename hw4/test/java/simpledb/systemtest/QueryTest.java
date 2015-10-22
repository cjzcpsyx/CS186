package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	@Test(timeout=20000) public void queryTest() throws IOException, DbException, TransactionAbortedException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.		
		final int IO_COST = 101;
		
//		HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
		
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		TableStats.setTableStats("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		TableStats.setTableStats("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		TableStats.setTableStats("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		TableStats.setTableStats("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));

//		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser p = new Parser();
		p.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		p.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	}
	
	/**
	 * Build a large series of tables; then run the command-line query code and execute a query.
	 * The number of tables is large enough that the query will only succeed within the
	 * specified time if a join method faster than nested-loops join is available.
	 * The tables are also too big for a query to be successful if its query plan isn't reasonably efficient,
	 * and there are too many tables for a brute-force search of all possible query plans.
	 */
	// Not required for Lab 4
	@Test(timeout=60000) public void greedyJoinTest() throws IOException, DbException, TransactionAbortedException {
		final int IO_COST = 103;
		
		HashMap<String, TableStats> stats = new HashMap<String,TableStats>();

		String tables = "abcdefghijklmnop";
		int counter = 100;
		for (char table: tables.toCharArray()) {
			ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
			HeapFile heapFile = SystemTestUtil.createRandomHeapFile(2, counter, null, tuples, "c");
			counter -= 3;
			Database.getCatalog().addTable(heapFile, String.valueOf(table));
			TableStats.setTableStats(String.valueOf(table), new TableStats(heapFile.getId(), IO_COST));
		}

		ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 1000, Integer.MAX_VALUE, null,
				smallHeapFileTuples, "c");

		ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < 1000; i++) {
			bigHeapFileTuples.add( smallHeapFileTuples.get( i%100 ) );
		}
		HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2, "c");
		Database.getCatalog().addTable(bigHeapFile, "bigTable");
		TableStats.setTableStats("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
//		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser p = new Parser();
		p.setTransaction(t);
		
		p.processNextStatement("SELECT * FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p " +
				"WHERE a.c0 = b.c0 AND b.c1 = c.c1 AND bigTable.c1 = p.c1 AND c.c0 = d.c0 AND d.c1 = e.c1 AND e.c0 = " +
				"f.c0 AND f.c1 = g.c1 AND g.c0 = h.c0 AND h.c1 = i.c1 AND i.c0 = j.c0 AND j.c1 = k.c1 AND k.c0 = l.c0" +
				" AND l.c1 = m.c1 AND m.c0 = n.c0 AND n.c1 = o.c1 AND o.c0 = p.c0;");
	}
}
