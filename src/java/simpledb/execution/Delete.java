package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

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
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});
    private boolean fetched = false;
    private BufferPool bufferPool = Database.getBufferPool();

    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
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
        if (fetched){
            return null;
        }else{
            fetched = true;
            int count = 0;
            Tuple toDelTuple = null;

            if (toDelTuple == null){
                toDelTuple = this.child.hasNext() ? this.child.next() : null;
            }
            while (toDelTuple != null){
                try {
                    bufferPool.deleteTuple(this.t, toDelTuple);
                    count += 1;
                }catch (IOException e){
                    throw new DbException("IOException when delete tuple");
                }
                toDelTuple = this.child.hasNext() ? this.child.next() : null;
            }
            Tuple toReturnTuple = new Tuple(this.td);
            toReturnTuple.setField(0, new IntField(count));
            return toReturnTuple;
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