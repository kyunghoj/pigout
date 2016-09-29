package edu.buffalo.cse.pigout.partitioner;

import edu.buffalo.cse.pigout.tools.PigOutUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

import java.util.List;

public class DataTransferCostEstimator extends LogicalPlanCostEstimator {

	protected final Log LOG = LogFactory.getLog(getClass());
    
    private PigContext context;

	private static final float OP_SCALER_FILTER = (float)0.5;
	private static final float OP_SCALER_DISTINCT = (float)0.25;
	private static final float OP_SCALER_LOAD = (float)0.000001;
    private static final float OP_SCALER_USERFUNC = (float) 0.10;

    private static final float NUMBER_OF_GROUPS = (float) 4.00;

    // For inner plan
    private float initCost = (float) 0.0;
    private boolean isInner = false;
    private OperatorPlan outerPlan = null;
    private LOForEach outerForEach = null;

    // when you get into inner plan
    protected DataTransferCostEstimator(OperatorPlan plan, PlanWalker walker, LOForEach op, float initCost)
        throws FrontendException {
        this(plan, walker);
        this.initCost = initCost;
        this.isInner = true;
        this.outerForEach = op;
        this.outerPlan = op.getPlan();
    }

    public DataTransferCostEstimator(PigContext context, OperatorPlan plan)
            throws FrontendException
    {
        this(plan, new DependencyOrderWalker(plan));
        this.context = context;
    }

	private DataTransferCostEstimator(OperatorPlan plan, PlanWalker walker)
			throws FrontendException {
		super(plan, walker);
	}

	private void updateAlias(LogicalRelationalOperator logRelOp) {
		String newAlias;
		List<Operator> preds = logRelOp.getPlan().getPredecessors(logRelOp);
		if (preds == null) {
			/* If the predecessor is null, set a unique alias */
			newAlias =  logRelOp.hashCode() + "null_pigout";
		} else {
			newAlias = ((LogicalRelationalOperator)preds.get(0)).getAlias() + "_pigout";
		}
		logRelOp.setAlias(newAlias);
	}

	private boolean updateCostTable(Operator op, Float cost) {
		this.costTable.put(op, cost);
		LogicalRelationalOperator logRelOp = (LogicalRelationalOperator)op;
		if (logRelOp.getAlias() == null) {
		    updateAlias(logRelOp);
		}
		LOG.debug(logRelOp.getAlias() + " = " + logRelOp.getName() + " => " + cost);
		    return false;
	}

	public void visit(LOStore store) throws FrontendException {
		Float costEstimate = getPredCost(store);
		this.updateCostTable(store, costEstimate);
	}

	public void visit(LOLoad load) throws FrontendException {
		FileSpec fs = load.getFileSpec();
		String namenode = getNameNode(fs);
		
		long size;
        size = PigOutUtils.getHDFSFileSize(fs.getFileName(), namenode);
		float costEstimate = OP_SCALER_LOAD * size;
		this.updateCostTable(load, costEstimate);
	}
	
	public void visit(LOFilter op) throws FrontendException {
		float costEstimate = OP_SCALER_FILTER * getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}   

	public void visit(LOJoin op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}

	public void visit(LOForEach op) throws FrontendException {
        float costEstimate;
        LogicalPlan innerPlan = op.getInnerPlan();
        DataTransferCostEstimator estimator;
        estimator = new DataTransferCostEstimator(innerPlan, new DependencyOrderWalker(innerPlan), op, getPredCost(op));
        estimator.visit();
        costEstimate = estimator.getCostTable().get(innerPlan.getSinks().get(0));
        this.updateCostTable(op, costEstimate);
	}

	public void visit(LOGenerate op) throws FrontendException {
		float total_pred_cost = 0;
		int total_pred_fields;
        boolean isUserFunc = false;

		LogicalPlan lp = (LogicalPlan) op.getPlan();
		List<Operator> preds = lp.getPredecessors(op);

        List<LogicalExpressionPlan> outExprPlans = op.getOutputPlans();
        for (LogicalExpressionPlan exprPlan : outExprPlans) {
            List<Operator> exprPlanSources = exprPlan.getSources();
            for (Operator exprSrc : exprPlanSources) {
                if (exprSrc instanceof UserFuncExpression) {
                    isUserFunc = true;
                }
            }
        }

		if (!preds.isEmpty()) {
			for (Operator pred : preds) {
				if (pred instanceof LogicalRelationalOperator) {
					LogicalRelationalOperator predLO = (LogicalRelationalOperator) pred;
					total_pred_cost += costTable.get(predLO);
				}
			}
		} else {
            LOG.error("Invalid logical plan: no predecessors for LOGenerate.");
			throw new FrontendException();
		}

        List<LogicalFieldSchema> result_fields = op.getSchema().getFields();
        float generate_weight = 1;
        /*
         * If generate has 'group' in output, we estimate that the number of rows
         * reduce by a factor of the number of groups, which is arbitrarily defined as 4
         */

        for (LogicalFieldSchema field : result_fields) {
            if (field.toString().startsWith("group")) {
                generate_weight = (float) 1.0 / NUMBER_OF_GROUPS;
                break;
            }
        }

        Operator outerPred = this.outerPlan.getPredecessors(this.outerForEach).get(0);
        try {
            total_pred_fields = ((LogicalRelationalOperator)outerPred).getSchema().size();
        } catch (NullPointerException npe) {
            if (((LogicalRelationalOperator) outerPred) instanceof LOLoad) {
                LOLoad loadOp = (LOLoad) outerPred;
                if (loadOp.getFileSpec().getFileName().endsWith("widerow"))
                    total_pred_fields = 500;
                else
                    throw npe;
            } else
                throw npe;
        }

		float cost = total_pred_cost * generate_weight * ( (float) 1 / total_pred_fields);

        if (isUserFunc)
            cost = cost * OP_SCALER_USERFUNC;

        this.updateCostTable(op, cost);
	}

    public void visit(LOInnerLoad op) throws FrontendException {
		float costEstimate;
        if (this.initCost != 0.0)
            costEstimate = this.initCost;
        else
            costEstimate = getPredCost(op);

        this.updateCostTable(op, costEstimate);
	}

	public void visit(LOCube op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}

	public void visit(LOCogroup op) throws FrontendException { 
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LOSplit op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LOSplitOutput op) throws FrontendException {
		int no_of_splits = op.getSchema().getFields().size();
		float costEstimate = getPredCost(op) / no_of_splits;
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LOSort op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LORank op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LODistinct op) throws FrontendException {
		float costEstimate = OP_SCALER_DISTINCT * getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LOLimit op) throws FrontendException {
		float costEstimate = getPredCost(op);
        this.updateCostTable(op, costEstimate);
	}
	
	public void visit(LOCross op) throws FrontendException {
		LogicalPlan lp = (LogicalPlan) op.getPlan();
		float pred_cost_product = 1;
		List<Operator> preds = lp.getPredecessors(op);
		if (!preds.isEmpty()) {
			for (Operator pred : preds) {
				pred_cost_product *= costTable.get(pred);
			}
		}
        this.updateCostTable(op, pred_cost_product);
	}
	
	public void visit(LOUnion op) throws FrontendException {
		LogicalPlan lp = (LogicalPlan) op.getPlan();
		float pred_cost_union = 0;
		List<Operator> preds = lp.getPredecessors(op);
		/*
		List<Operator> succs = lp.getSuccessors(op);
		if (!succs.isEmpty()) {
			for (Operator succ : succs) {
				System.out.println("succ = " + succ);
			}
		}
		*/
		if ( !preds.isEmpty() ) {
			for (Operator pred : preds) {
				LOG.debug("LOUnion's Pred = " + pred);
				float pred_val = costTable.get(pred);
				pred_cost_union += pred_val;
			}
		}
        this.updateCostTable(op, pred_cost_union);
	}
}
