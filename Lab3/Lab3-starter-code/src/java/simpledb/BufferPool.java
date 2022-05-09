package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class LockManager {
	
	public class PageLock {
		private PageId pid;
		private Permissions perm;
		public ArrayList<TransactionId> lockHolder;
		
		public PageLock(PageId pid, Permissions perm) {
			this.pid = pid;
			this.perm = perm;
			this.lockHolder = new ArrayList<>();
		}
		
		public void setPermission(Permissions perm) {
			this.perm = perm;
		}
		
		public PageId getPid() {
			return this.pid;
		}
		
		public Permissions getPerm() {
			return this.perm;
		}
		
		public boolean addHolder(TransactionId tid) {
			if(perm == Permissions.READ_ONLY) {
				if(!lockHolder.contains(tid)) {
					lockHolder.add(tid);
				}
				return true;
			}else{
				// Add holder to exclusive lock only if no one holds the lock
				if(lockHolder.size() == 0) {
					lockHolder.add(tid);
					return true;
				}
				return false;
			}
		}
		
		public boolean upgradeHolder(TransactionId tid) {
			if(perm == Permissions.READ_ONLY && lockHolder.size() == 1 && lockHolder.contains(tid)) {
				perm = Permissions.READ_WRITE;
				return true;
			}
			return false;
		}
	}
	
	private ConcurrentHashMap<PageId, PageLock> pageLockTable;
	private ConcurrentHashMap<TransactionId, ArrayList<PageId>> transactionTable;
	private Graph DepGraph;
	
	public LockManager() {
		pageLockTable = new ConcurrentHashMap<>();
		DepGraph = new Graph();
		transactionTable = new ConcurrentHashMap<>();
	}
	
	public synchronized boolean detectCycle() {
		return DepGraph.checkCycle();
	}
	
	public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
		
		if(perm == Permissions.READ_ONLY) {
			// If a shared lock is requested
			if(pageLockTable.containsKey(pid)) {
				// If there exists a page lock for this page
				if(pageLockTable.get(pid).getPerm() == Permissions.READ_ONLY) {
					// Grant shared lock when the page is held by another transaction with a shared lock
					updateTransactionTable(tid, pid);
					assert pageLockTable.get(pid).addHolder(tid) == true;
					return true;
				} else {
					// Wait if exclusive lock held by other transaction
					return holdsLock(tid, pid);
				}
			}else {
				// No lock has been granted for the page
				PageLock pl = new PageLock(pid, Permissions.READ_ONLY);
				pl.addHolder(tid);
				pageLockTable.put(pid, pl);
				updateTransactionTable(tid, pid);
				return true;
			}
		}else if(perm == Permissions.READ_WRITE) {
			// If an exclusive lock is requested
			if(pageLockTable.containsKey(pid)) {
				// Check the type of the lock being held
				if(pageLockTable.get(pid).getPerm() == Permissions.READ_WRITE) {
					// If exclusive lock held by another transaction, 
					// add dependency to the dependency graph and return false
					return holdsLock(tid, pid);
				}
				else {
					// If there exists a shared lock for this page, tries to upgrade the lock
					return pageLockTable.get(pid).upgradeHolder(tid);
				}
			}else {
				PageLock pl = new PageLock(pid, Permissions.READ_WRITE);
				pl.addHolder(tid);
				pageLockTable.put(pid, pl);
				updateTransactionTable(tid, pid);
				return true;
			}
		}
		return false;
	}
	
	public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
		// Remove transaction table
		if (transactionTable.containsKey(tid)) {
			transactionTable.get(tid).remove(pid);
			if (transactionTable.get(tid).size() == 0) {
				transactionTable.remove(tid);
			}
		}
		
		
		// Remove lock table
		if(pageLockTable.containsKey(pid) && pageLockTable.get(pid).lockHolder.contains(tid)){
			pageLockTable.get(pid).lockHolder.remove(tid);
			if(pageLockTable.get(pid).lockHolder.isEmpty()) {
				// if no more transaction hold lock to the page, remove it from the table
				pageLockTable.remove(pid);
			}
			return true;
		}
		return false;
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
		return pageLockTable.containsKey(pid) && pageLockTable.get(pid).lockHolder.contains(tid);
	}
	
	private synchronized void updateTransactionTable(TransactionId tid, PageId pid) {
		if (transactionTable.containsKey(tid)) {
            if (!transactionTable.get(tid).contains(pid)) {
                transactionTable.get(tid).add(pid);
            }
        } else {
            // no entry tid
            ArrayList<PageId> lockList = new ArrayList<PageId>();
            lockList.add(pid);
            transactionTable.put(tid, lockList);
        }
	}
	
	// return the id of transaction that currently holds exclusive lock to page pid  	
	private synchronized TransactionId getXLockHolder(PageId pid) {
		if (pageLockTable.get(pid).getPerm() == Permissions.READ_WRITE){
			return pageLockTable.get(pid).lockHolder.get(0);
		}
		return null;
	}
	
	// add dependency edge from tid to the transaction that currently holds exclusive lock to page pid 
	public synchronized void addDependency(TransactionId tid, PageId pid) {
		TransactionId XlockHolder = getXLockHolder(pid);
		if(XlockHolder!=null) {
			DepGraph.addDep(getXLockHolder(pid), tid);	
		}		
	}
	
	// removes all edges to tid in the dependency graph
	public synchronized void removeDependency(TransactionId tid) {
		DepGraph.removeDep(tid);
	}
}



/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	public final int MAX_PAGES;

	private LinkedHashMap<PageId, Page> bp_map;
	
	private LockManager lock_manager;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		MAX_PAGES = numPages;
		bp_map = new LinkedHashMap<>(MAX_PAGES, .75f, true);
		lock_manager = new LockManager();
	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned. If there is insufficient space in the buffer pool, a page
	 * should be evicted and the new page should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		boolean lockGranted = lock_manager.acquireLock(tid, pid, perm);
		if(!lockGranted) {
			lock_manager.addDependency(tid, pid);
		}
		int cnt = -1;		
		while(!lockGranted) {
			cnt+=1;
			if(cnt == 10) { // run cycle detection every second
				if(lock_manager.detectCycle()) { // abort current transaction and raise exception if cyclic 										
					try{
						transactionComplete(tid, false);
						throw new TransactionAbortedException();
					}catch(IOException e) {
						e.printStackTrace();
						throw new DbException("");
					}
				}				
				cnt = 0;				
			}
			try {
				Thread.sleep(100); // try to acquire lock every 0.1s 
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
			lockGranted = lock_manager.acquireLock(tid, pid, perm);			
		}
		
		if (bp_map.containsKey(pid)) {
			return bp_map.get(pid);
		}
		HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
		if (bp_map.size() >= MAX_PAGES) {
			evictPage();
		}
		HeapPage fetched_page = (HeapPage) hf.readPage(pid);
		bp_map.put(pid, fetched_page);
		return fetched_page;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		if (lock_manager.holdsLock(tid, pid)) {
			assert lock_manager.releaseLock(tid, pid) == true;
		}
		
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return lock_manager.holdsLock(tid, p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		
		if (commit) { // if commit
			flushPages(tid); // releasing done in flushPages
		} else { // if abort
			// set copy of bp_map to prevent concurrent modification exception
			Set<Map.Entry<PageId, Page>> bpMapEntries = new HashSet<>(bp_map.entrySet()); 
			
			for(Map.Entry<PageId, Page> p: bpMapEntries) {
				PageId pid = p.getKey();			
				if (lock_manager.holdsLock(tid, pid)) { 
					// if page is locked by this transaction, get the version on disk
					// HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
					// bp_map.put(pid, hf.readPage(pid));
					discardPage(pid);
					// release the page
					releasePage(tid, pid); // this is not working yet
					
				}
			}
		}		

	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will acquire
	 * a write lock on the page the tuple is added to and any other pages that are
	 * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
	 * cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirtied_pages = file.insertTuple(tid, t);
		// replace all the affected pages by the new version
		for (Page page : dirtied_pages) {
			page.markDirty(true, tid);
			PageId pid = page.getId();
			if (!bp_map.containsKey(pid)) {
				getPage(tid, page.getId(), Permissions.READ_WRITE);
			}
			bp_map.put(pid, page);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from and any other pages that are updated. May
	 * block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1

		int table_id = t.getRecordId().getPageId().getTableId();
		HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(table_id);
		ArrayList<Page> dirtied_pages = file.deleteTuple(tid, t);
		// mark affected pages as dirty
		for (Page page : dirtied_pages) {
			page.markDirty(true, tid);
			if (!bp_map.containsKey(page.getId()))
				getPage(tid, page.getId(), Permissions.READ_WRITE);
			bp_map.put(page.getId(), page);
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
	 * dirty data to disk so will break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		if (bp_map.size() > 0) {
			for(Map.Entry<PageId, Page> p: bp_map.entrySet()) {
				this.flushPage(p.getKey());
			}
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in its
	 * cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from the
	 * cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		if (bp_map.containsKey(pid)) {
			bp_map.remove(pid);
		}
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		if (bp_map.containsKey(pid)) {
			HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
			HeapPage fetched_page = null;
			for (Map.Entry<PageId, Page> p : bp_map.entrySet()) { // use entrySet to avoid ConcurrentModificationException for LinkedHeapMap
				if(pid == p.getKey()) {
					fetched_page = (HeapPage) p.getValue();
				}
			}
			TransactionId tid = fetched_page.isDirty();
			if (tid != null) {
				hf.writePage(fetched_page);
				fetched_page.markDirty(false, tid);
			}
		} else {
			throw new IOException("pid not in buffer");
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		// All pages touched by tid are flushed and then released.
		
		for(Map.Entry<PageId, Page> p: bp_map.entrySet()) { // for each page in buffer
			PageId pid = p.getKey();
			if (lock_manager.holdsLock(tid, pid)) { // if page locked by this transaction
				flushPage(pid);
				releasePage(tid, pid);
			}
		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		for (Map.Entry<PageId, Page> p: bp_map.entrySet()) {
			if (p.getValue().isDirty()==null) { // if page is clean
				PageId pid = p.getKey();
				try {
					flushPage(pid);
				} catch (IOException e) {
					e.printStackTrace();
				}
				bp_map.remove(pid);
				return;
			}
		}
		throw new DbException("There is no page to evict");
	}

}
