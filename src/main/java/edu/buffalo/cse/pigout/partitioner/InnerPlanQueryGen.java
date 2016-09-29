package edu.buffalo.cse.pigout.partitioner;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class InnerPlanQueryGen extends LogicalRelationalNodesVisitor {
	private int count = 0;
	
	private StringBuilder result;
	
	protected InnerPlanQueryGen(OperatorPlan plan, PlanWalker walker, StringBuilder sb)
			throws FrontendException {
		super(plan, walker);
		this.result = sb;
	}
	
	public void visit(LOInnerLoad op) throws FrontendException {
		;
	}

	public void visit(LOForEach op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}

	public void visit(LOFilter op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOJoin op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOCogroup op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOCross op)  throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LODistinct op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOLimit op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOSort op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOSplit op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public void visit(LOUnion op) throws FrontendException {
		result.append( LogicalToQuery.visit(op) );
	}
	
	public String get() {
		return result.toString();
	}
}
