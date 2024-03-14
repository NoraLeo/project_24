package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupAggregates;
    private HashMap<Field, Integer> groupCounts;
    private int noGroupAggregate;
    private int noGroupCount;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupAggregates = new HashMap<>();
        this.groupCounts = new HashMap<>();
        this.noGroupAggregate = 0;
        this.noGroupCount = 0;
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
                
                if (groupField == null) {
                    // No grouping
                    noGroupAggregate = aggregate(noGroupAggregate, tup.getField(afield).hashCode());
                    noGroupCount++;
                } else {
                    // Grouping
                    if (!groupAggregates.containsKey(groupField)) {
                        groupAggregates.put(groupField, tup.getField(afield).hashCode());
                        groupCounts.put(groupField, 1);
                    } else {
                        int currentValue = groupAggregates.get(groupField);
                        int newValue = aggregate(currentValue, tup.getField(afield).hashCode());
                        groupAggregates.put(groupField, newValue);
                        groupCounts.put(groupField, groupCounts.get(groupField) + 1);
                    }
                }
    
    }

    @Override
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();

        if (gbfield == NO_GROUPING) {
            Tuple tuple = new Tuple(Database.getCatalog().getTupleDesc(NO_GROUPING));
            tuple.setField(0, new IntField(noGroupAggregate));
            tuples.add(tuple);
        } else {
            for (Field groupField : groupAggregates.keySet()) {

                Tuple tuple = new Tuple(Database.getCatalog().getTupleDesc(gbfield));
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(groupAggregates.get(groupField)));
                tuples.add(tuple);
            }
        }

        return new TupleIterator(Database.getCatalog().getTupleDesc(gbfield), tuples);
    }

    private int aggregate(int value1, int value2) {
        switch (what) {
            case MIN:
                return Math.min(value1, value2);
            case MAX:
                return Math.max(value1, value2);
            case SUM:
                return value1 + value2;
            case AVG:
                return value1 + value2;
            case COUNT:
                return value1 + 1;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator");
        }
    }
}


