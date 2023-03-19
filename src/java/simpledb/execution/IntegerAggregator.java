package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op operator;
    private HashMap<Field, Integer> aggregatedTable = new HashMap<>();
    private HashMap<Field, Integer> fieldCountHm = new HashMap<>();
    private static Field PLACEHOLDER_FIELD = new IntField(NO_GROUPING);
    private boolean hasGroupings;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(
            int gbfield, Type gbfieldtype,
            int afield, Op what) {

        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.operator = what;
        this.hasGroupings = gbfield != NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = PLACEHOLDER_FIELD;
        if (hasGroupings) {
            groupField = tup.getField(this.gbfield);
        }
        int tupleVal = ((IntField) tup.getField(afield)).getValue();
        switch (this.operator) {
            case AVG: {
                int total = this.aggregatedTable.getOrDefault(groupField, 0);
                int prevCount = this.fieldCountHm.getOrDefault(groupField, 0);
                aggregatedTable.put(groupField, total + tupleVal);
                fieldCountHm.put(groupField, prevCount + 1);
                break;
            }
            case MAX: {
                int prevValue = this.aggregatedTable.getOrDefault(groupField, Integer.MIN_VALUE);
                aggregatedTable.put(groupField, Math.max(prevValue, tupleVal));
                break;
            }
            case MIN: {
                int prevValue = this.aggregatedTable.getOrDefault(groupField, Integer.MAX_VALUE);
                aggregatedTable.put(groupField, Math.min(prevValue, tupleVal));
                break;
            }
            case SUM: {
                int prevValue = this.aggregatedTable.getOrDefault(groupField, 0);
                aggregatedTable.put(groupField, prevValue + tupleVal);
                break;
            }
            case COUNT: {
                int prevValue = this.aggregatedTable.getOrDefault(groupField, 0);
                aggregatedTable.put(groupField, prevValue + 1);
                break;
            }
            default:
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (hasGroupings) {
            td = new TupleDesc(new Type[] { this.gbfieldType, Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        }

        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : aggregatedTable.entrySet()) {
            Tuple tup = new Tuple(td);
            int aggVal = entry.getValue();
            if (this.operator == Op.AVG) {
                int count = this.fieldCountHm.get(entry.getKey());
                aggVal = aggVal / count;
            }
            if (hasGroupings) {
                tup.setField(0, entry.getKey());
                tup.setField(1, new IntField(aggVal));
            } else {
                tup.setField(0, new IntField(aggVal));
            }
            tuples.add(tup);
        }

        return new TupleIterator(td, tuples);

    }
}
