package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator agg;
    private OpIterator aggIter;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.aggIter = null;
        this.agg = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (gfield == -1) {
            return Aggregator.NO_GROUPING;
        }
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (this.gfield == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return child.getTupleDesc().getFieldName(this.gfield);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
                if (this.aggIter == null) {
                    Type groupByFieldType = null;
                    if (this.gfield != Aggregator.NO_GROUPING) {
                        groupByFieldType = this.child.getTupleDesc().getFieldType(this.gfield);
                    }
        
                    if (this.child.getTupleDesc().getFieldType(this.afield) == Type.INT_TYPE) {
                        this.agg = new IntegerAggregator(this.gfield, groupByFieldType, this.afield, this.aop);
                    } else if (this.child.getTupleDesc().getFieldType(this.afield) == Type.STRING_TYPE) {
                        this.agg = new StringAggregator(this.gfield, groupByFieldType, this.afield, this.aop);
                    } else {
                        throw new DbException("This type of iterator is not supported");
                    }
        
                    this.child.open();
        
                    while (this.child.hasNext()) {
                        Tuple nextTuple = this.child.next();
                        this.agg.mergeTupleIntoGroup(nextTuple);
                    }
                    this.aggIter = this.agg.iterator();
                }
                this.aggIter.open();
                super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggIter.hasNext()) {
            return this.aggIter.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggIter = this.agg.iterator();
        this.aggIter.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggFieldName = this.aggregateFieldName() + " (" +
                this.child.getTupleDesc().getFieldName(this.afield) + ")";

        if (this.gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[] { this.child.getTupleDesc().getFieldType(this.afield) },
                    new String[] { aggFieldName });
        } else {
            return new TupleDesc(
                    new Type[] {
                            this.child.getTupleDesc().getFieldType(this.gfield),
                            this.child.getTupleDesc().getFieldType(this.afield)
                    },
                    new String[] {
                            this.child.getTupleDesc().getFieldName(this.gfield),
                            aggFieldName
                    });
        }
    }

    public void close() {
        super.close();
        this.child.close();
        this.aggIter.close();
        this.aggIter = null;
        this.aggIter = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
