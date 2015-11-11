package simpledb;

import java.util.HashMap;

/**
 * The Update operator. Update reads tuples from its child operator and updates Fields.
 */
public class Update extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private TupleDesc returnTD;
    private TransactionId tid;
    private boolean processed=false;
    private HashMap<String, Field> updates;

    /**
     * Constructor specifying the transaction that this update belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this update runs in
     * @param child
     *            The child operator from which to read tuples for update
     * @param updates
     *            A map of updates to perform for each column
     */
    public Update(TransactionId t, DbIterator child, HashMap<String, Field> updates) {
        this.child = child;
        this.tid = t;
        this.updates = updates;

        // we return a 1-field tuple
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        this.returnTD = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        return returnTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        child.open();
    }

    /**
     * Updates tuples as they are read from the child operator. Updates are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of updated records.
     * @see Database#getBufferPool
     * @see BufferPool#updateTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        if (processed)
            return null;

        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            TupleDesc tDesc = t.getTupleDesc();
            for (String col : updates.keySet()) {
                t.setField(tDesc.fieldNameToIndex(col), updates.get(col));
            }
            Database.getBufferPool().updateTuple(tid, t);
            count++;
        }

        // finished scanning
        // generate a new "update count" tuple
        Tuple tup = new Tuple(returnTD);
        tup.setField(0, new IntField(count));
        processed=true;
        return tup;
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
