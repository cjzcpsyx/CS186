package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		return null;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		return null;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME
		return null;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME
		return null;
	}


}