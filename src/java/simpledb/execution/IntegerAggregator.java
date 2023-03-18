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

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        boolean noGrouping = this.gbfield == NO_GROUPING;
        Field tuplefield = PLACEHOLDER_FIELD;
        int tupleValue = ((IntField) tup.getField(this.afield)).getValue();
        if (!noGrouping) {
            tuplefield = tup.getField(this.gbfield);
        }
        switch (this.operator) {
            case COUNT:
                int count = this.aggregatedTable.getOrDefault(tuplefield, 0);
                this.aggregatedTable.put(tuplefield, count + 1);
            case SUM:
                int sum = this.aggregatedTable.getOrDefault(tuplefield, 0);
                this.aggregatedTable.put(tuplefield, sum + tupleValue);

            case AVG:
                int average = this.aggregatedTable.getOrDefault(tuplefield, 0);
                int fieldCount = this.fieldCountHm.getOrDefault(tuplefield, 0);
                int newAverage = average + tupleValue / (fieldCount + 1);
                this.aggregatedTable.put(tuplefield, newAverage);

            case MIN:
                int min = this.aggregatedTable.getOrDefault(tuplefield, 0);
                if (tupleValue < min)
                    this.aggregatedTable.put(tuplefield, tupleValue);

            case MAX:
                int max = this.aggregatedTable.getOrDefault(tuplefield, 0);
                if (tupleValue > max)
                    this.aggregatedTable.put(tuplefield, tupleValue);

            default:
                return;
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
        // some code goes here
        TupleDesc aggTupleDesc;
        ArrayList<Tuple> tuples = new ArrayList<>();

        if ((this.gbfield != Aggregator.NO_GROUPING)) {
            aggTupleDesc = new TupleDesc(new Type[] { this.gbfieldType, Type.INT_TYPE });
        } else {
            aggTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        }

        for (Map.Entry<Field, Integer> groupAggregateEntry : this.aggregatedTable.entrySet()) {
            Tuple tuple = new Tuple(aggTupleDesc);

            if ((this.gbfield != Aggregator.NO_GROUPING)) {
                tuple.setField(0, groupAggregateEntry.getKey());
                tuple.setField(1, new IntField(groupAggregateEntry.getValue()));
            } else {
                tuple.setField(0, new IntField(groupAggregateEntry.getValue()));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(aggTupleDesc, tuples);
    }

}
