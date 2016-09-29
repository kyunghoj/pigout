package edu.buffalo.cse.pigout.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;

public class PigOutPartitionOptimizer {
	private static final int MAX_OPTIMIZATION_ATTEMPTS = 10;
	boolean finalOptimizedPlan = false;
	List <PigOutLogicalChunks> chunks;
	
	public PigOutPartitionOptimizer(List <PigOutLogicalChunks> chunks) {
		this.chunks = chunks;
	}
	
	public void addChuck(PigOutLogicalChunks chunk) {
		this.chunks.add(chunk);
	}

	public List<PigOutLogicalChunks> getChucks() {
		return this.chunks;
	}
	
	private void updateDependency(PigOutLogicalChunks oldDep, PigOutLogicalChunks newDep,
			List <PigOutLogicalChunks> optimizedChunks) {
        System.out.println("replacing old Chunk ID = " + oldDep.getChunkId());
        System.out.println("with new Chunk ID = " + newDep.getChunkId());
		for (PigOutLogicalChunks chunk : this.chunks) {
			if (chunk.predecessors.contains(oldDep)) {
		        System.out.println("Replacing dependency in Chunk ID " + chunk.getChunkId());
				chunk.predecessors.remove(oldDep);
				chunk.predecessors.add(newDep);
			}
		}
		for (PigOutLogicalChunks chunk : optimizedChunks) {
			if (chunk.predecessors.contains(oldDep)) {
		        System.out.println("Replacing dependency in OPT:Chunk ID " + chunk.getChunkId());
				chunk.predecessors.remove(oldDep);
				chunk.predecessors.add(newDep);
			}
		}
	}
	
	public void optimize() throws IOException {
		for (int i = 0; i < chunks.size(); i++) {
			LogicalPlanPrinter lpPrinter_old = new LogicalPlanPrinter(chunks.get(i).getPlan(), System.out);
            System.out.println("~~~~~~~~~ Before merge script: " + i + " ~~~~~~~~~~~~");
            System.out.println("Chunk ID = " + chunks.get(i).getChunkId() + " Cluster " + chunks.get(i).cluster.getnodeName());
            System.out.println("Dependency List:");
            for (int j = 0; j < chunks.get(i).getPredecessors().size(); j++) {
            	System.out.println("Chunk " + chunks.get(i).getPredecessors().get(j).getChunkId());
            }
            lpPrinter_old.visit();
		}
		int no_attempts = 0;
		while((finalOptimizedPlan == false) && (no_attempts < MAX_OPTIMIZATION_ATTEMPTS)) {
			System.out.println(">>>>>>>>>> Optimizing Plan, run " + no_attempts);
			optimizeOnce();
            System.out.println("New Dependency List:");
			for (int i = 0; i < chunks.size(); i++) {
	            System.out.println("Chunk ID = " + chunks.get(i).getChunkId());
				for (int j = 0; j < chunks.get(i).getPredecessors().size(); j++) {
					System.out.println("Depends on Chunks " + chunks.get(i).getPredecessors().get(j).getChunkId());
				}
			}
			no_attempts++;
		}
		for (int j = 0; j < chunks.size(); j++) {
			LogicalPlanPrinter lpPrinter_old = new LogicalPlanPrinter(chunks.get(j).getPlan(), System.out);
            System.out.println("~~~~~~~~~ After merge script: " + j + " ~~~~~~~~~~~~");
            lpPrinter_old.visit();
		}
	}

	private void optimizeOnce() throws IOException {
		System.out.println(">>>>>Optimization called on " + chunks.size() + " Scripts");
		finalOptimizedPlan = true;
		
		List <PigOutLogicalChunks> optimizedChunks = new ArrayList<PigOutLogicalChunks>();
		optimizedChunks.add(this.chunks.remove(0));
		PigOutLogicalChunks oldChunk = null;
		boolean merged = false;
		int i = -1;
		for (PigOutLogicalChunks chunk: this.chunks) {
			i++;
			merged = false;
			int j = -1;
			for (PigOutLogicalChunks optChunk: optimizedChunks) {
				j++;
				if(optChunk.isMergeable(chunk)) {
					System.out.println(">>>>> Trying to merge 1: opt-" + j + " With chunk-" + i);
					optChunk.merge(chunk);
					updateDependency(chunk, optChunk, optimizedChunks);
					merged = true;
					finalOptimizedPlan = false;
					break;
				} else if (chunk.isMergeable(optChunk)) {
					System.out.println(">>>>> Trying to merge 2: chunk-" + i + " With opt-" + j);
					chunk.merge(optChunk);
					updateDependency(optChunk, chunk, optimizedChunks);
					oldChunk = optChunk;
					finalOptimizedPlan = false;
					break;
				}
				System.out.println(">>>>> Could not merge opt-" + j + " With chunk-" + i);
			}
			if (merged) {
				System.out.println("Continuing");
				continue;
			}
			if (oldChunk != null) {
				optimizedChunks.remove(oldChunk);
			}
			//System.out.println(">>>>> Adding chunk to OPT");
			optimizedChunks.add(chunk);
		}
		//System.out.println(">>>>> Finished merging. No of scripts post merge = " + optimizedChunks.size());
		this.chunks = optimizedChunks; 
		/*
		for (int j = 0; j < chunks.size(); j++) {
			LogicalPlanPrinter lpPrinter_old = new LogicalPlanPrinter(chunks.get(j).getPlan(), System.out);
            System.out.println("~~~~~~~~~ after merge Script: " + j + " ~~~~~~~~~~~~");
            lpPrinter_old.visit();
		}
		*/
	}
}