package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import java.util.concurrent.locks.Lock;  
import java.util.concurrent.locks.ReentrantLock;

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

    File file;
    TupleDesc td;
    int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.numPage = (int) file.length() / BufferPool.getPageSize();
        // some code goes here
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
     * Returns an ID uniquely                                                                                                                                                                                                  e. Implementation note:
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
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        int pgNo = pid.getPageNumber();
        byte[] buf = new byte[pageSize];

        // pid invalid
        if (pid == null || pgNo > numPages() || pgNo < 0){
            throw new IllegalArgumentException();
        }

        try {
            if (pgNo == numPages()){ // pid reaches the end of this file. Create an empty page
                numPage += 1;
                return new HeapPage((HeapPageId)pid, HeapPage.createEmptyPageData());
            }
            // read file
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            randomAccessFile.seek(pgNo * pageSize);
            if(randomAccessFile.read(buf)==-1){
                randomAccessFile.close();
                return null;
            }

            heapPage = new HeapPage((HeapPageId) pid, buf);
            randomAccessFile.close();
            return heapPage;
        } catch (IOException e){
            throw new RuntimeException(e);        
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rws");
        randomAccessFile.skipBytes(pgNo * Database.getBufferPool().getPageSize());
        randomAccessFile.write(page.getPageData());
        if ( pgNo >= this.numPage){
            this.numPage = pgNo + 1;
        }
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPage;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!t.getTupleDesc().equals(this.td)){
            throw new DbException("The tupledesc doesn't match.");
        } 
            List<Page> pgList = new ArrayList<>();
            BufferPool bufferPool = Database.getBufferPool();
            for (int i=0; i<this.numPage; i++){
                HeapPageId pid = new HeapPageId(getId(), i); 
                HeapPage pg = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
                if (pg.getNumEmptySlots() == 0){ // page full
                    continue;
                }
                pg.insertTuple(t);
                pg.markDirty(true, tid);
                pgList.add(pg);
                return pgList;    
            }
            // add a new page
            HeapPageId pid = new HeapPageId(getId(), this.numPage); 
            HeapPage pg = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_ONLY);
            pg.insertTuple(t);
            pg.markDirty(true, tid);
            pgList.add(pg);
            // this.numPage += 1;
            return pgList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId()){ // pageid doesn't match
            throw new DbException("The tupele is not a member of the file.");
        }    
        ArrayList<Page> pgList = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        pgList.add(page);
        return pgList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        int pgNo = 0;
        BufferPool bufferPool = Database.getBufferPool();
        Permissions perm = Permissions.READ_ONLY;
        TransactionId tid;
        Iterator<Tuple> it;
        HeapFile file;
        
        HeapFileIterator(HeapFile f, TransactionId tid){
            this.tid = tid;
            this.file = f;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException{
            HeapPageId heapPageId = new HeapPageId(getId(), pgNo);
            HeapPage page = (HeapPage) this.bufferPool.getPage(this.tid, heapPageId, perm);
            this.it = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null){ // iterator isn't open
                return false;
            }
            if(it.hasNext()){ // tuples in this page are available
                return true;
            }
            if (this.pgNo+1 >= numPages()){ // no more page
                return false;
            }
            while (!it.hasNext() && this.pgNo < this.file.numPages() - 1){ // find next page with tuples
                this.pgNo++;
                HeapPageId heapPageId = new HeapPageId(getId(), this.pgNo);
                HeapPage page = (HeapPage) this.bufferPool.getPage(tid,heapPageId,this.perm);
                if (page == null){
                    continue;
                }
                this.it = page.iterator();
                if(this.it.hasNext()){
                    return true;
                }    
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }           
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.pgNo = 0;
            this.it = null;
        }
    }

}