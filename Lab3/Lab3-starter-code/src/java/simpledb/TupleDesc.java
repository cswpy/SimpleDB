package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	
	private ArrayList<TDItem> TDArray;
	private int numFields;
	
	
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
    
//    public static class TDItemIterator implements Iterator<TDItem> {
//    	
//    	private int cur = 0;
//    	
//    	@Override
//    	public boolean hasNext() {
//    		return TDArray.length > cur;
//    	}
//    	
//    	@Override
//    	public TDItem next() {
//    		if (this.hasNext()) {
//    			cur += 1;
//    			return TDArray[cur];
//    		}
//    		throw new NoSuchElementException();
//    	}
//    }
    

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDArray.iterator();
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
        if(typeAr != null && typeAr.length > 0){
            this.numFields = typeAr.length;
            this.TDArray = new ArrayList<TDItem>();
            for(int i=0; i<this.numFields; i++) {
        	    TDArray.add(new TDItem(typeAr[i], fieldAr[i]));
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
        this(typeAr, new String[typeAr.clone().length]);
    }
    
    public TupleDesc(ArrayList<TDItem> td_array) {
    	this.TDArray = td_array;
    	this.numFields = td_array.size();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
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
    	if(i < this.numFields && i>=0) {
    		return TDArray.get(i).fieldName;
    	}
        throw new NoSuchElementException();
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
    	if(i < this.numFields && i>=0) {
    		return TDArray.get(i).fieldType;
    	}
        throw new NoSuchElementException();
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
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
    	String field_name;
    	for(int i=0; i<this.numFields; i++) {
    		field_name = TDArray.get(i).fieldName;
    		if(field_name != null && field_name.equals(name)) {
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
        for(TDItem item : this.TDArray) {
        	size += item.fieldType.getLen();
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
    	ArrayList<TDItem> td_array = new ArrayList<TDItem>();
    	td_array.addAll(td1.TDArray);
    	td_array.addAll(td2.TDArray);
    	return new TupleDesc(td_array);
//    	int newLen = td1.numFields + td2.numFields;
//    	Type[] type_arr = new Type[newLen];
//    	String[] field_arr = new String[newLen];
//        for(int i=0; i<td1.numFields; i++) {
//        	type_arr[i] = td1.TDArray[i].fieldType;
//        	field_arr[i] = td1.TDArray[i].fieldName;
//        }
//        for(int i=0; i<td2.numFields; i++) {
//        	type_arr[i+td1.numFields] = td2.TDArray[i].fieldType;
//        	field_arr[i+td1.numFields] = td2.TDArray[i].fieldName;
//        }
//        return new TupleDesc(type_arr, field_arr);
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
    // Might be able to use default equals() in ArrayList by implementing equals() in TDItem class
    public boolean equals(Object o) {
        // some code goes here
    	if (o instanceof TupleDesc) {
    		TupleDesc td2 = (TupleDesc) o;
    		if(this.numFields == td2.numFields) {
    			for(int i=0; i<this.numFields; i++) {
    				if (this.TDArray.get(i).fieldType != td2.TDArray.get(i).fieldType) {
    					return false;
    				}
    			}
    			return true;
    		}
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
    	StringBuffer res = new StringBuffer();
    	for(TDItem item : this.TDArray) {
    		res.append(item.toString());
    		res.append(", ");
    	}
    	int startPos = res.length()-2;
    	int endPos = res.length();
    	res.delete(startPos, endPos);
        return res.toString();
    }
}
