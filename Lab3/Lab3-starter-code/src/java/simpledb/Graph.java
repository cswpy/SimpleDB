package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

  
class Graph {        
    private ConcurrentHashMap<TransactionId, ArrayList<TransactionId>> DepGraph;
  
    public Graph() {
    	DepGraph = new ConcurrentHashMap<TransactionId, ArrayList<TransactionId>>();
    }
    
    
	/**
		Add dependency edge T2->T1  
	 *
	 * @param T1 the ID of transaction with the lock
	 * @param T2 the ID of the transaction waiting for T1 to unlock
	 */
	public void addDep(TransactionId t1, TransactionId t2) {
    	if(DepGraph.containsKey(t1)) {
    		ArrayList<TransactionId> dependents = DepGraph.get(t1);
    		if(!dependents.contains(t2)) {
    			dependents.add(t2);
    		}
    	}else {
    		ArrayList<TransactionId> dependent = new ArrayList<>();
    		dependent.add(t2);
    		DepGraph.put(t1, dependent);
    	}
    }
	
	/**
		Remove all dependency edges to tid
	 *
 	* @param T1 the ID of transaction that committed/aborted 
 	*/
	public void removeDep(TransactionId tid) {
    	DepGraph.remove(tid);
    }
    
	/**
		Recursive DFS function for cycle detection.
	*/
	public boolean dfsCycle(TransactionId tid, HashSet<TransactionId> visited, HashSet<TransactionId> rec) {
    	
        if(rec.contains(tid)) {
        	return true;
        }        
        if(visited.contains(tid)) {
        	return false;
        }
        rec.add(tid);
        visited.add(tid);
 

        if(DepGraph.containsKey(tid)) {
            Iterator<TransactionId> tid_iter = DepGraph.get(tid).iterator();
            while(tid_iter.hasNext()) {
            	if(dfsCycle(tid_iter.next(), visited, rec)) {
            		return true;
            	}
            }	
        }
        rec.remove(tid);
        return false;
    }
    
	/**
		Checks for cycle in the current dependency cycle
	*/
	public boolean checkCycle() {    	
    	HashSet<TransactionId> visited = new HashSet<TransactionId>();
    	HashSet<TransactionId> rec = new HashSet<TransactionId>();
    	for (TransactionId tid: DepGraph.keySet()){
    		 if(dfsCycle(tid, visited, rec)) {
    			 return true;
    		 }
    	}
    	return false;
    }
}