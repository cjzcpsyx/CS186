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
    }

    public Predicate getPredicate() {
        // IMPLEMENT ME
        return null;
    }

    /**
     * Returns the schema of the operator.
     */
    public TupleDesc getTupleDesc() {
        // IMPLEMENT ME
        return null;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // IMPLEMENT ME
    }

    public void close() {
        // IMPLEMENT ME
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // IMPLEMENT ME
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
