package edu.buffalo.cse.pigout.partitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DepthFromSources extends LogicalRelationalNodesVisitor {

    protected final Log log = LogFactory.getLog(getClass());
    private Map<Operator, Integer> depths;

    protected DepthFromSources(OperatorPlan plan, PlanWalker walker)
        throws FrontendException {
        super(plan, walker);
    }

    public DepthFromSources(OperatorPlan plan) 
        throws FrontendException {
        this(plan, new DepthFirstWalker(plan));
        depths = new HashMap<>();
    }

    public Map<Operator, Integer> getDepths() {
        return this.depths;
    }

    private void calcDepth(LogicalRelationalOperator lop) throws FrontendException {

        List<Operator> predecessors = this.plan.getPredecessors(lop);

        if (predecessors == null || predecessors.size() == 0) {
            depths.put(lop, 0);
            return;
        }

        int minDepth = 9999;
        for (Operator pred : predecessors) {
            if (depths.get(pred) == null) continue;

            int d = depths.get(pred) + 1;
            if (minDepth > d) {
                minDepth = d;
            }
        }

        log.debug(lop.getAlias() + "'s depth: " + minDepth);
        depths.put(lop, minDepth);

    }

    public void visit(LOLoad op) throws FrontendException {
        depths.put(op, 1);
    }

    public void visit(LOStore op) throws FrontendException {
		calcDepth(op);  
	}

	public void visit(LOFilter op) throws FrontendException {
		calcDepth(op);  
	}

	public void visit(LOUnion op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LOJoin op) throws FrontendException {
		calcDepth(op);
	}

	public void visit(LOForEach op) throws FrontendException {
		calcDepth(op);
	}       

	public void visit(LOGenerate op) throws FrontendException {
		calcDepth(op);
	}           

	public void visit(LOInnerLoad op) throws FrontendException {
		calcDepth(op);
	}

	public void visit(LOCube op) throws FrontendException {
		calcDepth(op);
	}

	public void visit(LOCogroup op) throws FrontendException { 
		calcDepth(op);
	}
	
	public void visit(LOSplit op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LOSplitOutput op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LOSort op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LORank op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LODistinct op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LOLimit op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LOCross op) throws FrontendException {
		calcDepth(op);
	}
	
	
	public void visit(LOStream op) throws FrontendException {
		calcDepth(op);
	}
	
	public void visit(LONative op) throws FrontendException {
		calcDepth(op);
	}
}
