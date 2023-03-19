package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});
    private boolean fetched = false;


    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
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
        fetched = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        fetched = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count = 0;
        Tuple toInserTuple = null;
        BufferPool bufferPool = Database.getBufferPool();

        if(fetched){
            return null;
        }else{
            fetched = true;
            if(toInserTuple == null){
                toInserTuple = this.child.hasNext() ? this.child.next() : null;
            }
    
            while (toInserTuple != null){
                // if (this.child.getTupleDesc() != Database.getCatalog().getTupleDesc(this.tableId)){
                //     throw new DbException("Tuple Descriptor does not match");
                // }else{
                    try {
                        bufferPool.insertTuple(this.t, this.tableId, toInserTuple);
                        count += 1;
                    } catch (IOException e) {
                        throw new DbException("IOException when insert tuple");
                    }
                    
                // }
                toInserTuple = this.child.hasNext() ? this.child.next() : null;
            }
            Tuple toReuturnTuple = new Tuple(this.td);
            toReuturnTuple.setField(0, new IntField(count));
            return toReuturnTuple;
        }

        
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}