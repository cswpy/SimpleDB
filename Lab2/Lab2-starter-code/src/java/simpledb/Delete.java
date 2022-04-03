package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	private TransactionId transactionId;
	private OpIterator child;
	private boolean hasImplemented;


    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
		this.transactionId = t;
		this.child = child;
		this.hasImplemented = false;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.hasImplemented = true;
    	this.child.open();
    	try {
    		this.fetchNext();
    	} catch(DbException e)
    	{
    		throw new DbException("Error in open");
    	}
    	
    }

    public void close() {
        // some code goes here
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
		if(this.hasImplemented == true)
		{
			return null;
		}
		BufferPool bp = Database.getBufferPool();
		int cnt = 0;
		while (this.child.hasNext()) {
			Tuple tup = child.next();
			try {
				bp.deleteTuple(this.transactionId, tup);
			} catch (Exception e) {
				throw new DbException("Error in fetchNext");
			}
		}
		Tuple affectedTups = new Tuple(this.getTupleDesc());
		affectedTups.setField(0, new IntField(cnt));
		return affectedTups;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
