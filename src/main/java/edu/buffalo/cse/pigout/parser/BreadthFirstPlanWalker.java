/*
 * Feng Shen
 * 
 * This class is a helper for LPTOScriptTransformer
 * This class implements a breadthFirst plan visitor to 
 * visit the logical plan while maintaining dependency
 */
package edu.buffalo.cse.pigout.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class BreadthFirstPlanWalker {
	private LogicalPlan lp_;
	private List<Operator> fifo_ = new ArrayList<Operator>();

	public BreadthFirstPlanWalker(LogicalPlan lp) {
		this.lp_ = lp;
		topologicalSort(lp_);
	}
	
	public List<Operator> getOpList(){
		return this.fifo_;
	}

	private void topologicalSort(LogicalPlan plan) {
		Set<Operator> seen = new HashSet<Operator>();
		List<Operator> leaves = plan.getSinks();
		
		if(leaves == null) return;
		for(Operator op : leaves){
			doAllPredecessors(op, seen, fifo_);
		}
	}

	private void doAllPredecessors(Operator node, Set<Operator> seen,
			List<Operator> fifo) {
        if (!seen.contains(node)) {
            // We haven't seen this one before.
            Collection<Operator> preds = Utils.mergeCollection(lp_.getPredecessors(node), lp_.getSoftLinkPredecessors(node));
            if (preds != null && preds.size() > 0) {
                // Do all our predecessors before ourself
                for (Operator op : preds) {
                    doAllPredecessors(op, seen, fifo);
                }
            }
            // Now do ourself
            seen.add(node);
            fifo.add(node);
        }
	}
	
}
