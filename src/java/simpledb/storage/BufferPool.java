package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.*;

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
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     * @return 
     */
    public BufferPool(int numPages) {;
        this.numPage = numPages;
        this.pagePool = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
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
            if (perm == Permissions.READ_WRITE) {this.lockManager.obtainWriteLock(tid, pid);} 
            else if (perm == Permissions.READ_ONLY) {this.lockManager.obtainReadLock(tid, pid);} 
            else {throw new DbException("Invalid permission type.");}
            
            synchronized(this){
                if (this.pagePool.containsKey(pid)) {
                    Page page = this.pagePool.get(pid);
                    this.pagePool.remove(pid);
                    this.pagePool.put(pid, page);
                    return page; 
                }
                Page modifiedPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);

                if (this.pagePool.size() >= this.numPage) {evictPage();}
                if (perm == Permissions.READ_WRITE) {modifiedPage.markDirty(true, tid);}
                try {
                    updatePageInBufferPool(modifiedPage);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return modifiedPage;                
            }
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
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIdSet = this.lockManager.getPagesHeldBy(tid);
        if (pageIdSet == null) {return;}  
        if (commit) {
            for (PageId pageId : pageIdSet) {
                try {
                    this.flushPage(pageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for (PageId pageId : pageIdSet)
                this.discardPage(pageId);
        }
        this.lockManager.releaseAllLocks(tid);
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
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : this.lockManager.getPagesHeldBy(tid)) {
            this.flushPage(pid);
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

}