package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
		Join op = new Join(p, t1, t2);
		return op;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		Predicate p1 = new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1));
		Filter f =  new Filter(p1, t1);
		JoinPredicate p2 = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
		Join op = new Join(p2, f, t2);
		return op;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME
		Predicate p = new Predicate(2, Predicate.Op.GREATER_THAN, new IntField(1));
		Filter f = new Filter(p, t1);
		Aggregate op = new Aggregate(f, 1, 0, Aggregator.Op.MAX);
		return op;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME
		Aggregate ag1 = new Aggregate(t3, 0, -1, Aggregator.Op.COUNT);
		ag1.open();
		Predicate p1 = new Predicate(0, Predicate.Op.LESS_THAN, ag1.fetchNext().getField(0));
		ag1.close();
		Aggregate ag2 = new Aggregate(t3, 0, -1, Aggregator.Op.AVG);
		ag2.open();
		Predicate p2 = new Predicate(0, Predicate.Op.EQUALS, ag2.fetchNext().getField(0));
		ag2.close();
		Filter f1 = new Filter(p1, t1);
		Filter f2 = new Filter(p2, t2);
		JoinPredicate p3 = new JoinPredicate(1, Predicate.Op.GREATER_THAN_OR_EQ, 1);
		Join j = new Join(p3, t1, t2);
		OrderBy op = new OrderBy(0, false, j);
		return op;
	}


}