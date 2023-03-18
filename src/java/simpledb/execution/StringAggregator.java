package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private final Op operator = Op.COUNT;

    private HashMap<Field, Integer> aggregatedTable = new HashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what == Op.COUNT) {
            this.gbfield = gbfield;
            this.gbfieldType = gbfieldtype;
            this.afield = afield;
        } else
            throw new IllegalArgumentException();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.gbfield == NO_GROUPING) {
            Field tupleField = tup.getField(this.gbfield);
            Integer aggregatedFieldCount = this.aggregatedTable.getOrDefault(tupleField, 0);
            this.aggregatedTable.put(tupleField, aggregatedFieldCount + 1);
        } else {
            Integer aggregatedFieldCount = this.aggregatedTable.getOrDefault(afield, 0);
            this.aggregatedTable.put(null, aggregatedFieldCount + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {

        TupleDesc tupleDesc;
        if (this.gbfield == NO_GROUPING)
            tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });

        else {
            tupleDesc = new TupleDesc(new Type[] { this.gbfieldType, Type.INT_TYPE });
        }

        return new TupleIterator(null, getAggregatedList(tupleDesc));
    }

    private ArrayList<Tuple> getAggregatedList(TupleDesc tupleDesc) {
        ArrayList<Tuple> aggregatedList = new ArrayList<>();
        for (Map.Entry<Field, Integer> aggregatedEntry : aggregatedTable.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            if (this.gbfield == NO_GROUPING) {
                tuple.setField(this.afield, new IntField(aggregatedEntry.getValue()));
            } else {
                tuple.setField(0, aggregatedEntry.getKey());
                tuple.setField(this.afield, new IntField(aggregatedEntry.getValue()));
            }

        }
        return aggregatedList;

    }

}
