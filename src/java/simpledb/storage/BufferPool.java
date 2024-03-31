package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Set;


import javax.xml.crypto.Data;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPage;

    private final Map<PageId, Page> pagePool;
    lockManager lockManager = new lockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     * @return 
     */
    public BufferPool(int numPages) {;
        this.numPage = numPages;
        this.pagePool = new ConcurrentHashMap<>();
        
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
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
            if (this.pagePool.containsKey(pid)) {
                return this.pagePool.get(pid);
            }

            if (this.pagePool.size() >= this.numPage) {
                evictPage();
            }

            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            //lock the page
            synchronized (this){
                pagePool.put(pid, page);
            }
            this.pagePool.put(pid, page);
            return page;

            
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        lockManager.releaseLock(tid, null);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // Add implementation here
        // Check if the page is present in the page pool
        if (pagePool.containsKey(p)) {
            // Get the page from the page pool
            Page page = pagePool.get(p);
            // Check if the page is locked by the specified transaction
            return page.isDirty() == tid;
        }
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(commit && holdsLock(tid, null)){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                // Add any necessary code here
            }
        }else{
            for(PageId pid:this.pagePool.keySet()){
                if(this.pagePool.get(pid).isDirty()!=null){
                    try {
                        discardPage(pid);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
             DbFile file = Database.getCatalog().getDatabaseFile(tableId);

             List<Page> modifiedPages = file.insertTuple(tid, t);
             lockManager.acquireLock(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
             if (modifiedPages == null) {
                 throw new DbException("The specified tuple could not be inserted.");
             }

            // Mark all modified pages as dirty and update them in the buffer pool
            synchronized (this){
                for (Page modifiedPage : modifiedPages) {
                    modifiedPage.markDirty(true, tid);
                    updatePageInBufferPool(modifiedPage);
                }
            }
    }   

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = file.deleteTuple(tid, t);
        lockManager.acquireLock(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);  
        if (modifiedPages == null) {
            throw new DbException("The specified tuple could not be deleted.");
        }

        // Mark all modified pages as dirty and update them in the buffer pool
        for (Page modifiedPage : modifiedPages) {
            modifiedPage.markDirty(true, tid);
            updatePageInBufferPool(modifiedPage);
        }
    }
    private synchronized void updatePageInBufferPool(Page page) 
        throws DbException, IOException, TransactionAbortedException{
        if (!this.pagePool.containsKey(page.getId()) && this.pagePool.size() >= this.numPage) {
            throw new DbException("Buffer Pool is full.");
        }

        this.pagePool.remove(page.getId());
        
        // Update the page in the buffer pool
        this.pagePool.put(page.getId(), page);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page p:this.pagePool.values()){
            if(p.isDirty()!=null){
                flushPage(p.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pid == null) {
            return;
        }
        this.pagePool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page pg = this.pagePool.get(pid);

        if(this.pagePool.containsKey(pid)){
            TransactionId dirty=pg.isDirty();
            if (dirty != null) {
                Database.getLogFile().logWrite(dirty, pg.getBeforeImage(), pg);
                Database.getLogFile().force();
                DbFile pFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                pFile.writePage(pg);
                pg.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : this.pagePool.keySet()) {
            Page pg = this.pagePool.get(pid);
            if (pg.isDirty() == tid) {
                this.flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> iterator = this.pagePool.keySet().iterator();

        Page lastruPage = null;

        while (iterator.hasNext()) {
            Page pg = this.pagePool.get(iterator.next());
            if (pg.isDirty() == null) {
                lastruPage = pg;
            }
        }

        if (lastruPage == null) {
            throw new DbException("No pages to evict in the BufferPool");
        }

        try {
            this.flushPage(lastruPage.getId());
        } catch (IOException e) {
            throw new DbException("The specified page could not be flushed.");
        }
        this.pagePool.remove(lastruPage.getId());
    }

    private class lockManager {
        private Map<PageId, Set<TransactionId>> sharedLocks;
        private Map<PageId, TransactionId> exclusiveLocks;

        public lockManager() {
            this.sharedLocks = new ConcurrentHashMap<>();
            this.exclusiveLocks = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
            if (perm == Permissions.READ_ONLY) {
                return acquireSharedLock(tid, pid);
            } else {
                return acquireExclusiveLock(tid, pid);
            }
        }

        private synchronized boolean acquireSharedLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
            if (!this.sharedLocks.containsKey(pid)) {
                this.sharedLocks.put(pid, new HashSet<>());
            }

            if (this.exclusiveLocks.containsKey(pid) && this.exclusiveLocks.get(pid) != tid) {
                return false;
            }

            this.sharedLocks.get(pid).add(tid);
            return true;
        }

        private synchronized boolean acquireExclusiveLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
            if (this.sharedLocks.containsKey(pid) && this.sharedLocks.get(pid).size() > 1) {
                return false;
            }

            if (this.sharedLocks.containsKey(pid) && this.sharedLocks.get(pid).size() == 1 && !this.sharedLocks.get(pid).contains(tid)) {
                return false;
            }

            if (this.exclusiveLocks.containsKey(pid) && this.exclusiveLocks.get(pid) != tid) {
                return false;
            }

            this.exclusiveLocks.put(pid, tid);
            return true;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            if (this.sharedLocks.containsKey(pid) && this.sharedLocks.get(pid).contains(tid)) {
                this.sharedLocks.get(pid).remove(tid);
            }

            if (this.exclusiveLocks.containsKey(pid) && this.exclusiveLocks.get(pid) == tid) {
                this.exclusiveLocks.remove(pid);
            }
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            return (this.sharedLocks.containsKey(pid) && this.sharedLocks.get(pid).contains(tid)) ||
                (this.exclusiveLocks.containsKey(pid) && this.exclusiveLocks.get(pid) == tid);}

    }

}