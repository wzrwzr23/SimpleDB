package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc t1 = this.child1.getTupleDesc();
        TupleDesc t2 = this.child2.getTupleDesc();
        TupleDesc merged = TupleDesc.merge(t1, t2);
        return merged;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();

    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            this.child1.rewind();
            this.child2.rewind();
        } catch (Exception e) {
            throw e;
        }

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */


    private Tuple nexTuple1; private Tuple nexTuple2;

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (nexTuple1 == null) {
            nexTuple1 = (child1.hasNext()) ? child1.next() : null;
        }

        while (nexTuple1 != null) {
            while (this.child2.hasNext()){
                nexTuple2 = this.child2.next();
                if (this.p.filter(nexTuple1, nexTuple2)){
                    TupleDesc joinTupleDesc = this.getTupleDesc();
                    Tuple nexTuple = new Tuple(joinTupleDesc);

                    for(int i = 0; i < nexTuple1.getTupleDesc().numFields(); i++){
                        nexTuple.setField(i, nexTuple1.getField(i));
                    }
                    for(int i = nexTuple1.getTupleDesc().numFields(); i < this.getTupleDesc().numFields(); i++){
                        nexTuple.setField(i, nexTuple2.getField(i-nexTuple1.getTupleDesc().numFields()));
                    }

                    return nexTuple;
                }
            }

            child2.rewind();
            nexTuple1 = (child1.hasNext()) ? child1.next() : null;
        }

        return null;
    }
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
