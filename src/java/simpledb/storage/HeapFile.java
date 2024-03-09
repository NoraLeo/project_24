package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
	    this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
         return this.file;
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
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
         return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.seek(offset);
            raf.read(data, 0, BufferPool.getPageSize());
            raf.close();
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * Database.getBufferPool().getPageSize());
            raf.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int currentPageNo = 0; currentPageNo < this.numPages(); currentPageNo++) {
            HeapPageId pageId = new HeapPageId(this.getId(), currentPageNo);
            HeapPage currentPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (currentPage.getNumEmptySlots() > 0) {
                currentPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                currentPage.insertTuple(t);
                modifiedPages.add(currentPage);
                break;
            } 
        }

        if (modifiedPages.isEmpty()) {
            HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]);
            newPage.insertTuple(t);
            this.writePage(newPage);
            modifiedPages.add(newPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        ArrayList<Page> affectedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            if (i == pid.getPageNumber()) {
                HeapPage affectedPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                affectedPage.deleteTuple(t);
                affectedPages.add(affectedPage);
            }
        }

        if (affectedPages.isEmpty()) {
            throw new DbException("Tuple " + t + " is not in this table.");
        }
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageNum;
            private Iterator<Tuple> currentIterator;
            private TransactionId tid;
            private boolean isOpen;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPageNum = 0;
                currentIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currentPageNum), Permissions.READ_ONLY)).iterator();
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    return false;
                }
                if (currentIterator.hasNext()) {
                    return true;
                }
                while (currentPageNum < numPages() - 1) {
                    currentPageNum++;
                    currentIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currentPageNum), Permissions.READ_ONLY)).iterator();
                    if (currentIterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) {
                    throw new NoSuchElementException();
                }
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };
    }

}