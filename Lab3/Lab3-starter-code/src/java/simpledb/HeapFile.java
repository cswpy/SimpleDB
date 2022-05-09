package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File _f;
	private TupleDesc _td;
	private int table_id;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this._f = f;
    	this.table_id = f.getAbsoluteFile().hashCode();
    	this._td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this._f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return this.table_id;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return this._td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	Page page = null;
    	int page_size = BufferPool.getPageSize();
    	byte[] data = new byte[page_size];
    	try {
    		RandomAccessFile raf = new RandomAccessFile(this._f, "r");
    		raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
    		raf.read(data);
    		raf.close();
    		page = new HeapPage((HeapPageId)pid, data);
    	}
    	catch (IOException e){
    		e.printStackTrace();
    	}
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raf = new RandomAccessFile(this._f, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
            raf.close();
        }catch(IOException e) {
        	e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((double)_f.length() / (double) BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    		
    	// The array to return
    	ArrayList<Page> pageArr = new ArrayList<>();
    	
    	// find a page with empty slot to insert if there is any	
    	for (int i=0; i < numPages(); i++) {
    		HeapPageId curr_pid = new HeapPageId(table_id, i);
    		HeapPage curr_page = (HeapPage) Database.getBufferPool().getPage(tid, curr_pid, Permissions.READ_WRITE);
    		if (curr_page.getNumEmptySlots()>0) {
    			curr_page.insertTuple(t);
    			curr_page.markDirty(true, tid);
    			pageArr.add(curr_page);
    			return pageArr;
    		}
		// A lock on curr_page is granted to tid upon creation, so if we do not
    		// use the page then the lock should be released.
    		else {
    			Database.getBufferPool().releasePage(tid, curr_page.getId());
    		}
    	}
    	
    	// create a new page since all are full or there is no page
    	// num_pages = num_pages + 1;
    	HeapPageId new_pid = new HeapPageId(table_id, this.numPages());
		HeapPage new_page = new HeapPage(new_pid, HeapPage.createEmptyPageData());
		new_page.insertTuple(t);
		new_page.markDirty(true, tid);
		writePage(new_page);
    	pageArr.add(new_page);
		return pageArr;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	// not necessary for lab1
    	
    	// The array to return
    	ArrayList<Page> pageArr = new ArrayList<>();
    	if (t.getRecordId().getPageId().getTableId() != getId()) throw new DbException("Tuple not in this file");
    	
    	// find the page containing the tuple to be deleted	
    	PageId pid = t.getRecordId().getPageId();
  		HeapPage target_page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
  		target_page.deleteTuple(t);
    	target_page.markDirty(true, tid);
    	pageArr.add(target_page);
    	return pageArr;
    	
    	
    }

    // see DbFile.java for javadocs
    // Fetch the catalog and bufferpool from database, query the catalog 
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator{
    	
    	private TransactionId _tid;
    	private int page_idx;
    	private Iterator<Tuple> heap_page_iter;
    	private BufferPool bp;
    	   	
		public HeapFileIterator(TransactionId tid) {
    		this._tid = tid;
    		this.bp = Database.getBufferPool();
    	}
    	
    	
    	 // Do not use readPage but use BufferPool.getPage()
    	 public void open() throws DbException, TransactionAbortedException{
    		 page_idx = 0;
    		 HeapPageId pid = new HeapPageId(table_id, page_idx);
    		 HeapPage hp = (HeapPage) bp.getPage(_tid, pid, Permissions.READ_ONLY);
    		 heap_page_iter = hp.iterator();
    	 }
    	 
    	 public boolean hasNext() throws DbException, TransactionAbortedException {
    		 if (heap_page_iter != null) {
    			 
//        		 if(page_idx == null) {
//        			 return false;
//        		 }
        		 if(heap_page_iter.hasNext()) {
        			 return true;
        		 }else {
        			 if (++page_idx < (numPages())) {
//        				 page_idx += 1;
        				 HeapPageId pid = new HeapPageId(table_id, page_idx);
        				 HeapPage hp = (HeapPage) bp.getPage(_tid, pid, Permissions.READ_ONLY);
        				 heap_page_iter = hp.iterator();
        				 return heap_page_iter.hasNext();
        			 }
//        			 }else {
//        				 page_idx = null;
//        				 return false;
//        			 }
        		 }
    		 }
    		 return false;

    	 }
    	 
    	 public Tuple next() throws NoSuchElementException, DbException, TransactionAbortedException {
    		 if(hasNext()) { 
    			 return heap_page_iter.next();
    		 }
    		 else {
    			 throw new NoSuchElementException();
    		 }

    		 
    	 }
    	 
    	 public void rewind() throws DbException, TransactionAbortedException{
    		 this.open();
    	 }
    	 
    	 public void close() {
//    		 bp = null;
    		 heap_page_iter = null;
//    		 page_idx = null;
    	 }
    }

}

