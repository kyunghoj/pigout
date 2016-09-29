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
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

public class AssignClustersFromLoads extends LogicalRelationalNodesVisitor {

	protected final Log LOG = LogFactory.getLog(getClass());
	private Map<Operator, List<String>> map;
	private Map<LOLoad, Long> inputSize;
	
	protected AssignClustersFromLoads(OperatorPlan plan, PlanWalker walker)
			throws FrontendException {
		super(plan, walker);
	}

	public AssignClustersFromLoads(OperatorPlan plan)
			throws FrontendException {
		this(plan, new DependencyOrderWalker(plan));
		map = new HashMap<Operator, List<String>>();
		inputSize = new HashMap<LOLoad, Long>();
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
		FileSpec fs = op.getFileSpec();
		String namenode = getNameNode(fs);
		List<String> clusters = new LinkedList<String>();
		
		// LOAD cannot have multiple clusters to run
		if (map.containsKey(op)) {
			LOG.debug(op.getName() + "'s clusters: " + map.get(op));
			return;
		}
	
		clusters.add(namenode);
		map.put(op, clusters);
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
		// Let's not do this until we need it. It takes long, and
		// doesn't work without VPN on off-campus.
		/*
		long size = 0;
		try {
			size = PigOutUtils.getHDFSFileSize(fs.getFileName(), namenode);
			LOG.debug(fs.getFileName() + " : " + namenode + " : " + size + " bytes");
		} catch (IOException e) {
			size = -1;
			LOG.debug(fs.getFileName() + " : " + namenode + " : " + size + " bytes");
		}
		inputSize.put(op, size);
		*/
	}
	
	public void visit(LOStore op) throws FrontendException {
		// STORE rel into 'filepath';
		FileSpec fs = op.getFileSpec();
		String namenode = getNameNode(fs);
		List<String> clusters = new LinkedList<String>();
		
		// STORE cannot have multiple clusters to run
		if (map.containsKey(op)) {
			LOG.debug(op.getName() + "'s clusters: " + map.get(op));
			return;
		}

		clusters.add(namenode);
		map.put(op, clusters);
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
	}
	
	private void getPredsClusters(Operator op) {
		LogicalPlan lp = (LogicalPlan) op.getPlan();
		List<Operator> preds = lp.getPredecessors(op);
		List<String> opCls = map.get(op);
		if (opCls == null) 
			opCls = new LinkedList<String>();
		
		for (Operator pred : preds) {
			List<String> cls = map.get(pred);
			if (cls == null) {
				LogicalRelationalOperator lo = (LogicalRelationalOperator)pred;
				LOG.debug(lo.getAlias() + " was not assigned to a cluster.");
				continue;
			}
			for (String c : cls) {
				if (!opCls.contains(c)) opCls.add(c);
			}
		}
		map.put(op, opCls);
		LOG.debug(op.getName() + "'s clusters: " + map.get(op));
	}

	public void visit(LOFilter op) throws FrontendException {
		getPredsClusters(op);  
	}

	public void visit(LOUnion op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LOJoin op) throws FrontendException {
		getPredsClusters(op);
	}

	public void visit(LOForEach op) throws FrontendException {
		getPredsClusters(op);
	}       

	public void visit(LOGenerate op) throws FrontendException {
		getPredsClusters(op);
	}           

	public void visit(LOInnerLoad op) throws FrontendException {
		getPredsClusters(op);
	}

	public void visit(LOCube op) throws FrontendException {
		getPredsClusters(op);
	}

	public void visit(LOCogroup op) throws FrontendException { 
		getPredsClusters(op);
	}
	
	public void visit(LOSplit op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LOSplitOutput op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LOSort op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LORank op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LODistinct op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LOLimit op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LOCross op) throws FrontendException {
		getPredsClusters(op);
	}
	
	
	public void visit(LOStream op) throws FrontendException {
		getPredsClusters(op);
	}
	
	public void visit(LONative op) throws FrontendException {
		getPredsClusters(op);
	}
	
}
