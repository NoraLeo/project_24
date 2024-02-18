package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] TDItemList;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;
        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.asList(this.TDItemList).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.TDItemList = new TDItem[(typeAr.length)];
        for (int i = 0; i < typeAr.length; i++) {
            if (fieldAr[i] != null) {
                this.TDItemList[i]= new TDItem(typeAr[i], fieldAr[i]);
            } else {
                this.TDItemList[i]= new TDItem(typeAr[i], null);
            }
        }
        
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.TDItemList = new TDItem[(typeAr.length)];
        for (int i = 0; i < typeAr.length; i++) {
            this.TDItemList[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TDItemList.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.TDItemList[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return this.TDItemList[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int ans = -1;
        for (int i = 0; i < this.TDItemList.length; i++) {
            if (this.TDItemList[i].fieldName != null && this.TDItemList[i].fieldName.equals(name)) {
                ans = i;
                break;
            }
        }
        if (ans != -1) {
            return ans;
        } else {
            throw new NoSuchElementException("No field with a matching name is found");
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.TDItemList.length; i++) {
            size += this.TDItemList[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[len];
        TupleDesc newTD = new TupleDesc(typeAr);
        for (int i = 0; i < td1.numFields(); i++) {
            if (td1.getFieldName(i) != null) {
                newTD.TDItemList[i]= new TDItem( td1.getFieldType(i), td1.getFieldName(i) );
            } else {
                newTD.TDItemList[i]= new TDItem( td1.getFieldType(i), null);
            }
        }
        for (int i = td1.numFields(); i < len; i++) {
            if (td2.getFieldName(i - td1.numFields()) != null) {
                newTD.TDItemList[i]= new TDItem( td2.getFieldType(i - td1.numFields()), td2.getFieldName(i - td1.numFields()) );
            } else {
                newTD.TDItemList[i]= new TDItem(td2.getFieldType(i - td1.numFields()), null);
            }
        }
        return newTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // check if number of items in the TupleDesc is the same
        if (o instanceof TupleDesc) {
            TupleDesc td = (TupleDesc) o;
            if (td.numFields() != this.numFields()) {
                return false;
            }
            // check if the i-th type in this TupleDesc is equal to the i-th type in o for every i
            for (int i = 0; i < this.numFields(); i++) {
                if (td.getFieldType(i) != this.getFieldType(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
