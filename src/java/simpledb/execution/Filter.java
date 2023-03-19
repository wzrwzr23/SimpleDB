package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate p;
    private OpIterator child;
    private boolean openFlag;

    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
        this.openFlag = false;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        try{
            super.open();
            this.child.open();
            this.openFlag = true;
        }catch(Exception e){
            throw e;
        }
        
    }

    public void close() {
        // some code goes here
        try{
            super.close();
            this.child.close();
            this.openFlag = false;
        }catch(Exception e){
            throw e;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        
        if (!this.openFlag){
            throw new IllegalStateException();
        }else{
            this.child.rewind();
        }
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        
        while (this.child.hasNext()){
            Tuple nextTuple = this.child.next();
            if (this.p.filter(nextTuple)){
                return nextTuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};

    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
