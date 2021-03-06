package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	private TransactionId transactionId;
	private OpIterator child;
	private boolean hasImplemented;
	private int cnt;


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
		this.cnt = 0;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	this.child.open();
    	
    }

    public void close() {
        // some code goes here
    	super.close();
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	this.hasImplemented = false;
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
		if(this.hasImplemented) {
			return null;
		}
		BufferPool bp = Database.getBufferPool();
		while(this.child.hasNext()){
			Tuple tup = child.next();
			try {
				bp.deleteTuple(this.transactionId, tup);
				this.cnt+=1;
			}catch(Exception e) {
				throw new DbException("Db exception");
			}
		}
		this.hasImplemented = true;
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
    
//    @Override
//    public Tuple next() throws NoSuchElementException {
//    	try {
//			return this.fetchNext();
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new NoSuchElementException("error at next");
//		}
//    }
//    
//    public boolean hasNext() throws DbException, TransactionAbortedException {
//    	return this.child.hasNext();
//    }
    
}
