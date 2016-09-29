package edu.buffalo.cse.pigout.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
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
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class AssignClustersFromStores extends LogicalRelationalNodesVisitor {
	
	protected final Log LOG = LogFactory.getLog(getClass());
	
	private Map<Operator, List<String>> map;
	
	protected AssignClustersFromStores(OperatorPlan plan)
			throws FrontendException {
		super(plan, new ReverseDependencyOrderWalker(plan));
	}
	
	public AssignClustersFromStores(OperatorPlan plan, Map<Operator, List<String>> map)
			throws FrontendException {
		this(plan);
		this.map = map;
	}
	
	private static String getNameNode(FileSpec fs) {
		String filepath = fs.getFileName();
		// We assume inputs from HDFS. 
		// The following 'if' is not necessary, but we may need it
		// when we add other types
		if (filepath.startsWith("hdfs://")) {
			try {
				return filepath.substring("hdfs://".length()).split("/", 2)[0];
			} catch (Exception e) {
				return "localhost";
			}
		}
		
		return "localhost";
	}
	
	public Map<Operator, List<String>> getMap() {
		return this.map;
	}
	
	public void visit(LOLoad op) throws FrontendException {
		// do nothing
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
	}
	
	public void visit(LOStore op) throws FrontendException {
		// do nothing
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
	}
	
	private void getSuccsClusters(Operator op) {
		LogicalPlan lp = (LogicalPlan) op.getPlan();
		List<Operator> succs = lp.getSuccessors(op);
		List<String> opCls = map.get(op);
		if (opCls == null) { 
			// exception?
			LOG.error(op.getName() + "'s successor is not assigned to any cluster.");
		} 
		
		for (Operator succ : succs) {
			List<String> cls = map.get(succ);
			for (String c : cls) {
				if (!opCls.contains(c)) opCls.add(c);
			}
		}
		map.put(op, opCls);
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
	}
	
	public void visit(LOFilter op) throws FrontendException {
		getSuccsClusters(op);  
	}   

	public void visit(LOUnion op) throws FrontendException {
//		getSuccsClusters(op);
	}
	
	
	public void visit(LOJoin op) throws FrontendException {
//		getSuccsClusters(op);
	}

	public void visit(LOForEach op) throws FrontendException {
		getSuccsClusters(op);
	}       

	public void visit(LOGenerate op) throws FrontendException {
		getSuccsClusters(op);
	}           

	public void visit(LOInnerLoad op) throws FrontendException {
		getSuccsClusters(op);
	}

	public void visit(LOCube op) throws FrontendException {
		getSuccsClusters(op);
	}

	public void visit(LOCogroup op) throws FrontendException { 
//		getSuccsClusters(op);
	}
	
	public void visit(LOSplit op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LOSplitOutput op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LOSort op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LORank op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LODistinct op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LOLimit op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LOCross op) throws FrontendException {
//		getSuccsClusters(op);
	}
	
	
	public void visit(LOStream op) throws FrontendException {
		getSuccsClusters(op);
	}
	
	public void visit(LONative op) throws FrontendException {
		getSuccsClusters(op);
	}

}
