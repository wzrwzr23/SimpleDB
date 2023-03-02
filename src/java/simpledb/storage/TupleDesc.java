package simpledb.storage;

import simpledb.common.Type;
import simpledb.common.Utility;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> it = this.tdItemList.iterator();
        return it;
    }

    private static final long serialVersionUID = 1L;

    public ArrayList<TDItem> tdItemList;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        // some code goes here
        if (typeAr.length < 1) {
            System.out.println("Length of type array can't be 0.");
            throw new IllegalArgumentException();
        }

        ArrayList<TDItem> tdItemList = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            if (i < fieldAr.length) {
                tdItemList.add(new TDItem(typeAr[i], fieldAr[i]));
            } else {
                tdItemList.add(new TDItem(typeAr[i], null));
            }
        }
        this.tdItemList = tdItemList;
    }

    public ArrayList<TDItem> getTdItemList() {
        return this.tdItemList;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) throws IllegalArgumentException {
        // some code goes here
        if (typeAr.length < 1) {
            System.out.println("Length of type array can't be 0.");
            throw new IllegalArgumentException();
        }
        ArrayList<TDItem> tdItemList = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            tdItemList.add(new TDItem(typeAr[i], null));
        }
        this.tdItemList = tdItemList;
    }

    public TupleDesc(ArrayList<TDItem> tdItemList) throws IllegalArgumentException {
        // some code goes here
        if (tdItemList.size() == 0) {
            throw new IllegalArgumentException();
        }
        this.tdItemList = tdItemList;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        int len = this.tdItemList.size();
        return len;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            String name = this.tdItemList.get(i).fieldName;
            return name;
        } catch (NoSuchElementException e) {
            System.out.println("Index out of range.");
            return null;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference`.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {

            TDItem tdItem = this.tdItemList.get(i);
            return tdItem.fieldType;
        } catch (NoSuchElementException e) {
            System.out.println("Index out of range.");
            return null;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // try{
        for (int i = 0; i < this.tdItemList.size(); i++) {
            String s = this.tdItemList.get(i).fieldName;
            if (s == null) {
                if (s == name) {
                    return i;
                } else {
                    continue;
                }
            }
            if (s.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        Iterator<TDItem> it = this.iterator();
        while (it.hasNext()) {
            Type type = it.next().fieldType;
            size += type.getLen();
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
        ArrayList<TDItem> both = new ArrayList<TDItem>();
        ArrayList<TDItem> tdItemList1 = td1.getTdItemList();
        ArrayList<TDItem> tdItemList2 = td2.getTdItemList();
        both.addAll(tdItemList1);
        both.addAll(tdItemList2);
        return new TupleDesc(both);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) throws ClassCastException {
        // some code goes here
        if (o == null) {
            return false;
        }

        TupleDesc oTD;
        try {
            oTD = (TupleDesc) o;
        } catch (ClassCastException e) {
            System.out.println("Argument must be TupleDesc type.");
            return false;
        }

        int nf1 = this.numFields();
        int nf2 = oTD.numFields();

        if (nf1 == nf2) {
            for (int i = 0; i < nf1; i++) {
                Class c1 = this.tdItemList.get(i).fieldType.getClass();
                Class c2 = oTD.tdItemList.get(i).fieldType.getClass();
                if (c1 != c2) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;

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
        String output = "";
        Iterator<TDItem> it = this.iterator();
        while (it.hasNext()) {
            String next = it.next().toString();
            output += next;
        }
        return output;
    }
}
