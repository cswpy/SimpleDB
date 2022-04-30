package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId transactionId;
	private OpIterator child;
	private int tid;
	private boolean hasImplemented;
	private int cnt;

	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we
	 *                     are to insert.
	 */
	public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
		this.transactionId = t;
		this.child = child;
		this.tid = tableId;
		this.hasImplemented = false;
		this.cnt = 0;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return new TupleDesc(new Type[] { Type.INT_TYPE });
	}

	public void open() throws DbException, TransactionAbortedException {
		this.child.open();
		super.open();
		// some code goes here
	}

	public void close() {
		// some code goes here
		super.close();
		this.child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child.rewind();
	}

	/**
	 * Inserts tuples read from child into the tableId specified by the constructor.
	 * It returns a one field tuple containing the number of inserted records.
	 * Inserts should be passed through BufferPool. An instances of BufferPool is
	 * available via Database.getBufferPool(). Note that insert DOES NOT need check
	 * to see if a particular tuple is a duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or null if
	 *         called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
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
				bp.insertTuple(this.transactionId, this.tid, tup);
				cnt+=1;
			}catch(Exception e) {
				throw new DbException("Db exception");
			}
		}
		Tuple affectedTups = new Tuple(this.getTupleDesc());
		affectedTups.setField(0, new IntField(cnt));
		this.hasImplemented = true;
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
