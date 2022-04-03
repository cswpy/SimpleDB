package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int agField;
    private Op aggOp;
    private HashMap<Field, Integer> gb2val;
	
    
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
    	if (! (what == Op.COUNT)) {
    		throw new IllegalArgumentException("Only count aggregation is supported for string");
    	}
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.agField = afield;
    	this.aggOp = what;	
    	this.gb2val = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field agfield = tup.getField(agField);
    	if (agfield.getType() != Type.STRING_TYPE) {
    		throw new IllegalArgumentException("Cannot aggregate on non-string value");
    	}
    	
    	Field gbfield = null;
    	if(gbField != Aggregator.NO_GROUPING) {
    		gbfield = tup.getField(gbField);
    	}   	
		if(gb2val.containsKey(gbfield)) {
			int curr_cnt = gb2val.get(gbfield);
			gb2val.put(gbfield, curr_cnt+1);
		}else {
			gb2val.put(gbfield, 1);
		}
    	
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
        // some code goes here
    	Type[] new_types;
    	if (gbField == IntegerAggregator.NO_GROUPING) {
    		new_types = new Type[] {Type.INT_TYPE};
    	}else {
    		new_types = new Type[] {gbFieldType, Type.INT_TYPE};
    	}
    	TupleDesc newTd = new TupleDesc(new_types);
    	ArrayList<Tuple> tups = new ArrayList<Tuple>();
    	
    	for(Map.Entry<Field, Integer> set : gb2val.entrySet()) {
    		Tuple tup = new Tuple(newTd);
    		if (gbField == IntegerAggregator.NO_GROUPING) {
    			tup.setField(0, new IntField(set.getValue()));
    		}else {
    			tup.setField(0, set.getKey());
    			tup.setField(1, new IntField(set.getValue()));
    		}
    		tups.add(tup);
    	}
    	
        return new TupleIterator(newTd, tups);
    	
    }

}
