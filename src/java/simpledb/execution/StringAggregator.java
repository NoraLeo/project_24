package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupCounts;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.groupCounts = new HashMap<>();
        
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(this.gbfield);
        }
        int currentCount = this.groupCounts.getOrDefault(groupByField, 0);
        int updatedCount = ++currentCount;
        this.groupCounts.put(groupByField, updatedCount);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc groupAggregateTd;
        ArrayList<Tuple> tuples = new ArrayList<>();
        boolean hasGrouping = (this.gbfield != Aggregator.NO_GROUPING);

        if (hasGrouping) {
            groupAggregateTd = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        } else {
            groupAggregateTd = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        for (Map.Entry<Field, Integer> groupAggregateEntry: groupCounts.entrySet()) {
            Tuple groupCountsTuple = new Tuple(groupAggregateTd);

            // If there is a grouping, we return a tuple in the form {groupByField, aggregateVal}
            // If there is no grouping, we return a tuple in the form {aggregateVal}
            if (hasGrouping) {
                groupCountsTuple.setField(0, groupAggregateEntry.getKey());
                groupCountsTuple.setField(1, new IntField(groupAggregateEntry.getValue()));
            } else {
                groupCountsTuple.setField(0, new IntField(groupAggregateEntry.getValue()));
            }
            tuples.add(groupCountsTuple);
        }
        return new TupleIterator(groupAggregateTd, tuples);
    }

}
