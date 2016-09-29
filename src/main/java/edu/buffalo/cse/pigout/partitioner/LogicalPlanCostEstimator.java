package edu.buffalo.cse.pigout.partitioner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;


public class LogicalPlanCostEstimator extends LogicalRelationalNodesVisitor {

	protected final Log LOG = LogFactory.getLog(getClass());
	
	// costTable keeps the estimate of output size 
	protected Map<Operator, Float> costTable;
	
	public Map<Operator, Float> getCostTable() {
		return costTable;
	}

	protected LogicalPlanCostEstimator(OperatorPlan plan, PlanWalker walker)
			throws FrontendException {
		super(plan, walker);
		costTable = new HashMap<Operator, Float>();
	} 
	
	protected static String getNameNode(FileSpec fs) {
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
	
	protected float getPredCost(Operator op) {
		LogicalPlan lp = (LogicalPlan) op.getPlan();
		float total_pred_cost = 0;
		List<Operator> preds = lp.getPredecessors(op);
		if (preds != null && !preds.isEmpty()) {
			for (Operator pred : preds) {
				float cost = costTable.get(pred);
				total_pred_cost += cost;
			}
		}
		return total_pred_cost;
	}
	
}