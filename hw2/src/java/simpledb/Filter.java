package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // IMPLEMENT ME
        this.pred = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // IMPLEMENT ME
        return this.pred;
    }

    /**
     * Returns the schema of the operator.
     */
    public TupleDesc getTupleDesc() {
        // IMPLEMENT ME
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // IMPLEMENT ME
        super.open();
        this.child.open();
    }

    public void close() {
        // IMPLEMENT ME
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // IMPLEMENT ME
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // IMPLEMENT ME
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (pred.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
