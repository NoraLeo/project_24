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
    private TupleDesc childTd;
    private OpIterator aggIterator;

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
        super();
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.agg = null;
        this.childTd = child.getTupleDesc();
        this.aggIterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (this.gfield == -1) {
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
        if (this.gfield == -1) {
            return null;
        }
        return this.childTd.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(afield);
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
        super.open(); 
        if (this.aggIterator == null) {
            if (this.gfield == -1){
                this.agg = new IntegerAggregator(gfield, null, afield, aop);
            } else if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE){
                this.agg = new StringAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
            } else if (this.childTd.getFieldType(afield) == Type.INT_TYPE) {
                this.agg = new IntegerAggregator(gfield, this.childTd.getFieldType(gfield), afield, aop);
            } else {
                throw new DbException("Unsupported type");
            }
            child.open();
            while (child.hasNext()) {
                Tuple nexTuple = child.next();
                this.agg.mergeTupleIntoGroup(nexTuple);
            }
            child.close();
            this.aggIterator = this.agg.iterator();
        }
        this.aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggIterator.hasNext()) {
            Tuple nextTuple = this.aggIterator.next();
            return nextTuple;
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggIterator.rewind();
        this.aggIterator.open();
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
        if (gfield == -1) {
            return new TupleDesc(new Type[]{childTd.getFieldType(this.afield)}, new String[]{childTd.getFieldName(this.afield)});
        } else {
            return new TupleDesc(new Type[]{childTd.getFieldType(this.gfield), childTd.getFieldType(this.afield)},
                    new String[]{childTd.getFieldName(this.gfield), childTd.getFieldName(afield)});
        }
    }

    public void close() {
        super.close();
        if (this.aggIterator != null) {
            this.aggIterator.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (child != children[0]) {
            child = children[0];
        }
    }

}
