package edu.buffalo.cse.pigout.partitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

// TODO: Now it enumerates every assignment. But later 
// we can introduce security/privacy policy and generate
// assignments that comply with the policy.
public class EnumerateAssignments extends LogicalRelationalNodesVisitor {

	private Set<Map<Operator, String>> enumeration;
	private Map<Operator, List<String>> assignments;
	
	protected EnumerateAssignments(OperatorPlan plan, PlanWalker walker)
		throws FrontendException {
		super(plan, walker);
	}
	
	public EnumerateAssignments(OperatorPlan plan, Map<Operator, List<String>> assignments)
		throws FrontendException {
		this(plan, new DependencyOrderWalker(plan));
		this.assignments = assignments;
		this.enumeration = new HashSet<Map<Operator, String>>();
	}
	
	public Set<Map<Operator, String>> get() {
		return this.enumeration;
	}
	
	public void enumerate(Operator op) {
		// how many assignments are possible for this operator
		int n = this.assignments.get(op).size();
		
		if (this.enumeration.size() == 0) 
			this.enumeration.add(new HashMap<Operator, String>());
		
		List<Map<Operator, String>> newAssignments = new ArrayList<Map<Operator, String>>();
		
		for (Map<Operator, String> map : this.enumeration) {
			if (n == 1) {
				map.put(op, (String) this.assignments.get(op).toArray()[0]);
			} else {
				List<String> workerList = this.assignments.get(op);
				String[] workers = (String[]) workerList.toArray(new String[0]);
				
				for (int i = 1; i < n; i++) {
					Map<Operator, String> newMap = new HashMap<Operator, String>();
					newMap.putAll(map);
					newMap.put(op,  workers[i]);
					newAssignments.add(newMap);
				}
				map.put(op, workers[0]);
			}
		}
		this.enumeration.addAll(newAssignments);
	}
	
	@Override
	public void visit(LOLoad load) throws FrontendException {
		this.enumerate( (Operator)load);
	}

	@Override
	public void visit(LOFilter filter) throws FrontendException {
		this.enumerate( (Operator)filter);
	}

	@Override
	public void visit(LOStore store) throws FrontendException {
		this.enumerate( (Operator)store);
	}

	@Override
	public void visit(LOJoin join) throws FrontendException {
		this.enumerate( (Operator)join);
	}

	@Override
	public void visit(LOForEach foreach) throws FrontendException {
		this.enumerate( (Operator)foreach);
	}

	@Override
	public void visit(LOGenerate gen) throws FrontendException {
		this.enumerate( (Operator)gen);
	}

	@Override
	public void visit(LOInnerLoad load) throws FrontendException {
		this.enumerate( (Operator)load);
	}

	@Override
	public void visit(LOCube cube) throws FrontendException {
		this.enumerate( (Operator)cube);
	}

	@Override
	public void visit(LOCogroup loCogroup) throws FrontendException {
		this.enumerate( (Operator)loCogroup);
	}

	@Override
	public void visit(LOSplit loSplit) throws FrontendException {
		this.enumerate( (Operator)loSplit);
	}

	@Override
	public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
		this.enumerate( (Operator)loSplitOutput);
	}

	@Override
	public void visit(LOUnion loUnion) throws FrontendException {
		this.enumerate( (Operator)loUnion);
	}

	@Override
	public void visit(LOSort loSort) throws FrontendException {
		this.enumerate( (Operator)loSort);
	}

	@Override
	public void visit(LORank loRank) throws FrontendException {
		this.enumerate( (Operator)loRank);
	}

	@Override
	public void visit(LODistinct loDistinct) throws FrontendException {
		this.enumerate( (Operator)loDistinct);
	}

	@Override
	public void visit(LOLimit loLimit) throws FrontendException {
		this.enumerate( (Operator)loLimit);
	}

	@Override
	public void visit(LOCross loCross) throws FrontendException {
		this.enumerate( (Operator)loCross);
	}

	@Override
	public void visit(LOStream loStream) throws FrontendException {
		this.enumerate( (Operator)loStream);
	}

	@Override
	public void visit(LONative nativeMR) throws FrontendException {
		this.enumerate( (Operator)nativeMR);
	}
}
