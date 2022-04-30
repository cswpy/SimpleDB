package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int agField;
    private Op aggOp;
    private HashMap<Field, Integer> gb2val;
    private HashMap<Field, Integer[]> gb2sum_cnt;

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
        // some code goes here
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.agField = afield;
    	this.aggOp = what;
    	this.gb2val = new HashMap<>();
    	this.gb2sum_cnt = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field agfield = tup.getField(agField);
    	if (agfield.getType() != Type.INT_TYPE) {
    		throw new IllegalArgumentException("Cannot aggregate on non-integer value");
    	}
    	int agval = ((IntField)agfield).getValue();
    	Field gbfield = null;
    	if(gbField != Aggregator.NO_GROUPING) {
    		gbfield = tup.getField(gbField);
    	}
    	int newAggVal;
    	if (aggOp != Op.AVG) {
    		if(gb2val.containsKey(gbfield)) {
    			newAggVal = mergeAggregateVal(gb2val.get(gbfield), aggOp, agval);
    		}else {
    			if (aggOp == Op.COUNT) {
    				newAggVal = 1;
    			}else {
    				newAggVal = agval;
    			}
    		}
    	}else {
    		int sum=agval;
    		int cnt=1;
    		if(gb2val.containsKey(gbfield)) {
    			Integer[] oldArr = gb2sum_cnt.get(gbfield);
    			sum += oldArr[0];
    			cnt += oldArr[1];
    		}
    		gb2sum_cnt.put(gbfield, new Integer[]{sum, cnt});
    		newAggVal = sum / cnt;
    	}
    	gb2val.put(gbfield, newAggVal);
    	
    }
    
    private int mergeAggregateVal(int oldAggVal, Op aggOp, int newAggVal) {
    	switch(aggOp) {
    		case MAX:
    			return Math.max(oldAggVal, newAggVal);
    		case MIN:
    			return Math.min(oldAggVal, newAggVal);
    		case SUM:
    			return oldAggVal+newAggVal;
    		case COUNT:
    			return oldAggVal+1;
    		default:
    			throw new IllegalArgumentException("Illegal agggregation operator");
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
