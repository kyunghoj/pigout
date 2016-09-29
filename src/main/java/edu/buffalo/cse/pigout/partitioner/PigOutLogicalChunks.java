package edu.buffalo.cse.pigout.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

public class PigOutLogicalChunks {
    private final Log log = LogFactory.getLog(getClass());

    String alias;
	LogicalPlan lp;
	PigOutCluster cluster;
	int noMRjobs;
	long dataInputSize;
	long dataOutputSize;
	PigContext pigContext;
	List<PigOutLogicalChunks> predecessors;
	private String chunkId;

    public String getChunkId() {
		return chunkId;
	}
	public void setAlias(String alias) { this.alias = alias;}
    public String getAlias() { return this.alias;}

    public LogicalPlan getLogicalPlan() {
        return lp;
    }

	public long getDataInputSize() {
		return dataInputSize;
	}

	public void setDataInputSize(long dataInputSize) {
		this.dataInputSize = dataInputSize;
	}

	public long getDataOutputSize() {
		return dataOutputSize;
	}

	public void setDataOutputSize(long dataOutputSize) {
		this.dataOutputSize = dataOutputSize;
	}

	public int getNoMRjobs() {
		return noMRjobs;
	}

	public List<PigOutLogicalChunks> getPredecessors() {
		return predecessors;
	}

    public PigOutCluster getPigOutCluster() { return cluster; }

	public LogicalPlan getPlan() {
		return lp;
	}

	public PigOutLogicalChunks(LogicalPlan lp, PigContext pigContext, PigOutCluster cluster, long dataIn, long dataOut) {
		this.lp = lp;
		this.dataInputSize = dataIn;
		this.dataOutputSize = dataOut;
		this.pigContext = pigContext;
		this.noMRjobs = getNumberOfMRJobs();
		this.cluster = cluster;
		try {
			this.chunkId = lp.getSignature();
		} catch (FrontendException e) {
			System.out.println(">>>>> Unexpected Execption in assigning the Chunk ID");
			this.chunkId = "NULL";
		}
		predecessors = new ArrayList<PigOutLogicalChunks>();
	}

	public void addPredecessors(PigOutLogicalChunks pred) {
		if (!predecessors.contains(pred))
            predecessors.add(pred);
	}

	private int getNumberOfMRJobs() {
		HExecutionEngine mapRed = new HExecutionEngine(this.pigContext);
        PhysicalPlan php;
		MROperPlan mrp = null;
        try {
			php = mapRed.compile(lp, pigContext.getProperties());
	        MapReduceLauncher mapRedLaunch = new MapReduceLauncher();
			mrp = mapRedLaunch.compile(php, pigContext);
		} catch (IOException e) {
			log.error("Exception during compilation of a logical plan: ");
            log.error("\n" + lp);
			e.printStackTrace();
		}
		return mrp.size();
	}

	public void merge(PigOutLogicalChunks chunk) throws IOException {
		//make a copy of the old plan
		LogicalPlan oldLp = new LogicalPlan(lp);
		List<Operator> curLeaves = oldLp.getSinks();
		//System.out.println("Number of Leaves in this = " +  curLeaves.size());
		List<Operator> newSubRoots = chunk.getPlan().getSources();
		//System.out.println("Number of sources in chunk = " +  newSubRoots.size());
		
		for (Operator root : chunk.getPlan().getSources()) {
            lp.add(root);
			connectNodes(root);
        }
		
		//System.out.println(">>>>> Merge Added all nodes no of newSubRoots = " + newSubRoots.size() +
		//		" number of Current Leaves = " + curLeaves.size());
		for (Operator newSubRoot : newSubRoots) {
			for (Operator curLeaf: curLeaves) {
				//System.out.println("CurLeaf = " + curLeaf.getName() + " Name = " + curLeaf.toString());
				if (lp.getPredecessors(curLeaf) == null) {
					continue;
				}
				if (lp.getPredecessors(curLeaf).size() > 1) {
					/* Store should never have more than 1 predecessor */
					System.out.println("!!!!!!! Exception!!");
					throw new FrontendException();
				}
				Operator fromNode = lp.getPredecessors(curLeaf).get(0);
			//	System.out.println("Trying to Merge >>> From: " + ((LogicalRelationalOperator)fromNode).getAlias());
			//	System.out.println("Trying to Merge >>> To: " + ((LogicalRelationalOperator)newSubRoot).getAlias());
				if(((LogicalRelationalOperator)newSubRoot).getAlias() == 
						((LogicalRelationalOperator)fromNode).getAlias()) {
					for (Operator toNode : chunk.getPlan().getSuccessors(newSubRoot)) {
				//		System.out.println("Mering..");
						lp.connect(fromNode, toNode);
						lp.disconnect(newSubRoot, toNode);
					}
					lp.disconnect(fromNode, curLeaf);
					lp.remove(newSubRoot);
					lp.remove(curLeaf);
				}
			}
		}
		// update the predecessors of this chunk
		if ((predecessors.size() == 1) && (predecessors.get(0) == chunk)) {
			predecessors = chunk.getPredecessors();
		}
		this.noMRjobs = getNoMRjobs();
	}
	
	private void connectNodes(Operator node) throws FrontendException, IOException {

        List<Operator> operators = node.getPlan().getSuccessors(node);
        if (operators == null) {
            return;
		}
        List<Operator> successors =  new ArrayList<Operator>(operators);
        for (Operator succ : successors) {
        	lp.add(succ);
            lp.connect(node, succ);
        	connectNodes(succ);
        }
    }

	/*
	 * isMergable is not commutative: 
	 * A.merge(B) != B.merge(A) 
	 */
	public boolean isMergeable(PigOutLogicalChunks chunk) {
		// we merge the 2 scripts if they have exactly the same predecessors
		// or one depends on another
		if (!this.cluster.equals(chunk.cluster)) {
			System.out.println("2 Chunks assigned to differnt clusters, unmergable");
			return false;
		}
		if ((chunk.predecessors.size() - this.predecessors.size() != 0) &&
				(chunk.predecessors.size() - this.predecessors.size()) != 1) {
			System.out.println("2 chunks have different number predecessors, can't merge");
			return false;
		}
		for (PigOutLogicalChunks dep : chunk.getPredecessors()) {
			if (dep == this ) {
				continue;
			} else if (this.predecessors.contains(dep)) {
				continue;
			} else {
				System.out.println("2 chunks have different predecessors, can't merge");
				return false;
			}
		}
		return true;
	}
//		if ((chunk.predecessors.size() == 1) && (chunk.predecessors.get(0) == this)) {
//			System.out.println("Chunk depends only on this object, merging");
//			/* chuck depends only on this */
//			return true;
//		} else if ((chunk.predecessors.size() == this.predecessors.size()) &&
//			(chunk.predecessors.containsAll(this.predecessors))) {
//			System.out.println("2 Chunks have the same dependecies, merging");
//			/* Both the chunks have same dependecies */
//			return true;
//		} else {
//			if (chunk.getPredecessors().size() == 0) {
//				return false;
//			}
//			if (chunk.getPredecessors().contains(this)) {
//	}
}
