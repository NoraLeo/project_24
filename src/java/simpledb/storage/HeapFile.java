package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
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

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc td;
    private int fid;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs

    public Page readPage(PageId pid) {
        // some code goes here

        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "r");

            byte[] buffer = new byte[BufferPool.getPageSize()];

            // read to buffer at offset,length
            file.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            file.read(buffer, 0, buffer.length);
            file.close();

            return new HeapPage((HeapPageId) pid, buffer);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // "note that you do not necessarily have to implement writepag

        //todo last part

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {

            randomAccessFile.seek(page.getId().getPageNumber()*Database.getBufferPool().getPageSize());
            randomAccessFile.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        List<Page> pages = new ArrayList<>();

        // find a page with space
        for (int idx = 0; idx < numPages(); idx++ ){

            HeapPageId pid = new HeapPageId(getId(), idx);

            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);

            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                // todo repeat
                pages.add(page);
                return pages;
            } else {
                Database.getBufferPool().unsafeReleasePage(tid, pid);
            }
        }

        // If there are no existing pages, create a new page and add in the tuple
        byte[] header = new byte[BufferPool.getPageSize()];
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), header);
        newPage.insertTuple(t);
        this.writePage(newPage);

        pages.add(newPage);

        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        ArrayList<Page> pages = new ArrayList<>();

        for (int idx = 0; idx < numPages(); idx++) {
            if (idx == t.getRecordId().getPageId().getPageNumber()) {

                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
                page.deleteTuple(t);

                pages.add(page);
                return pages;

            }
        }

        throw new DbException("tuple not found");
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new AbstractDbFileIterator() {

            private int outer;
            private Iterator<Tuple> innerIterator;

            @Override
            // modified in lab2
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (innerIterator == null){
                    return null;
                }
                if (innerIterator.hasNext()) {
                    return innerIterator.next();
                }

                // find next page w ith tuples
                while (outer < numPages()-1) {
                    outer++;
                    innerIterator = getNewIterator();
                    if (innerIterator.hasNext()) {
                        return innerIterator.next();
                    } else if (innerIterator == null) {
                        return null;
                    }
                }

                return null; // No more tuples
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                outer = 0;
                innerIterator = getNewIterator();
            }

            private Iterator<Tuple> getNewIterator() {
                try {
                    return ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), outer), Permissions.READ_ONLY)).iterator();
                } catch (TransactionAbortedException e) {
                    throw new RuntimeException(e);
                } catch (DbException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                innerIterator = null;
                super.close();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
        };
    }

}