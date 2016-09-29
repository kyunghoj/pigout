package edu.buffalo.cse.pigout.partitioner;

public class PigOutCluster {
	String nodeName;
	int numNodes;
    int portNum;
	float execCostFactor;
	float dataTransferCostFactor;

	public String getnodeName() {
		return nodeName;
	}

    public int getPortNum() { return this.portNum; }
	
	public int getNumNodes() {
		return numNodes;
	}

	public float getExecCostFactor() {
		return execCostFactor;
	}

	public float getDataTransferCostFactor() {
		return dataTransferCostFactor;
	}

	@Override
	public boolean equals(Object cluster) {
		if (!(cluster instanceof PigOutCluster)) {
			return false;
		}
		return this.getnodeName().equals(((PigOutCluster)cluster).getnodeName());
	}
	
	public PigOutCluster(String nodeName, int portNum, int numNodes, float execCostFactor, float dataTransferCostFactor) {
		this.numNodes = numNodes;
        this.portNum = portNum;
		this.execCostFactor = execCostFactor;
		this.dataTransferCostFactor = dataTransferCostFactor;
		this.nodeName = nodeName;
	}
}
