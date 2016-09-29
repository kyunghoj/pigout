package edu.buffalo.cse.pigout.partitioner;

import edu.buffalo.cse.pigout.parser.LogicalExpressionPlanResolver;
import edu.buffalo.cse.pigout.parser.StringHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LogicalToQuery {

	private static final Log LOG = LogFactory.getLog( LogicalToQuery.class );
	
	protected static String parseFuncSpec(FuncSpec func) {
		StringBuilder sb = new StringBuilder();
		String className = func.getClassName();
		String[] args = func.getCtorArgs();
		
		sb.append(className + "(");
		
		if (args != null) {
			
			int i = 0;
			for (String arg : args) {
				if (i > 0) sb.append(", ");
				String argStr = StringHelper.escapeInputString(arg);
				sb.append("'" + argStr + "'");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	private static String parseGroupType(GROUPTYPE type) {
		StringBuilder sb = new StringBuilder();
		sb.append("'");
		switch (type) {
		case COLLECTED:
			sb.append("collected");
			break;
		case MERGE:
			sb.append("merge");
			break;
		case REGULAR:
			sb.append("regular");
			break;
		default:
			break;
		}
		sb.append("'");
		return sb.toString();
	}

    /*
     * Extract information from LOLoad's String representation
     * since RequiredFields is hidden from class LOLoad
     */
    private static int[] getRequiredFields(LOLoad op) {
        String load_str = op.toString();
        int fieldStartIdx;
        if (load_str.indexOf("RequiredFields:[") == -1)
            return null;
        fieldStartIdx = load_str.indexOf("RequiredFields:[") + "RequiredFields:[".length();

        int fieldEndIdx = load_str.length() - 1;
        String[] reqFieldsStr = load_str.substring(fieldStartIdx, fieldEndIdx).split(",");
        int [] reqFields = new int[reqFieldsStr.length];
        int j  = 0;
        for (String field : reqFieldsStr) {
            reqFields[j] = Integer.parseInt(field.trim());
            j++;
        }

        return reqFields;
    }

	public static String visit(LOLoad op) 
			throws FrontendException {

		StringBuilder sb = new StringBuilder();
		
		try {
            int[] reqFields = getRequiredFields(op);

			String alias = op.getAlias();
			// retrieve the output file name
			String filename = op.getSchemaFile();

			String input = filename;

			sb.append(alias + " = LOAD \'"+input+"\'");

			FuncSpec func = op.getFileSpec().getFuncSpec();
			if (func != null) {
				String funcStr = parseFuncSpec(func);
				sb.append(" USING " + funcStr);
			}

			// get schema
			LogicalSchema schema = op.getSchema();
			if (schema != null) {
				sb.append(" AS (");
				List<LogicalFieldSchema> fields = schema.getFields();
				int size = fields.size();
                int k = 0;
                if (reqFields == null) {
                    for (int i = 0; i < size; i++) {
                        LogicalFieldSchema lfs = fields.get(i);
                        if (lfs.alias == null)
                            sb.append("f" + i);
                        else
                            sb.append(lfs.alias);

                        if (i < size - 1)
                            sb.append(", ");
                    }
                } else {
                    for (int i = 0; i <= reqFields[reqFields.length - 1] && k < size; i++) {
                        LogicalFieldSchema lfs = fields.get(k);
                        if (i == (reqFields[k])) {
                            k++;
                            if (lfs.alias != null)
                                sb.append( lfs.alias );
                            else
                                sb.append("f" + i);
                        } else {
                            // insert columns which were removed by column prune optimization
                            sb.append("f" + i);
                        }
                        if (i < reqFields[reqFields.length - 1]) {
                            sb.append(", ");
                        }
                    }
                }
				sb.append(")");
			}

			sb.append(";");
		} catch (IOException e) {

			e.printStackTrace();
		}
		return sb.toString();
	}

	public static String visit(LOForEach op) throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String alias = op.getAlias();
        if (!Character.isLetter( alias.toCharArray()[0] )) {
            op.setAlias("fe_" + alias.replace('-', '_'));
            alias = op.getAlias();
        }

		sb.append(alias + " = FOREACH ");

		OperatorPlan lp = op.getPlan();
		List<Operator> preds = lp.getPredecessors(op);

		for (Operator pred : preds) {
			LogicalRelationalOperator lro = 
					(LogicalRelationalOperator)pred;
			String rel = lro.getAlias();
			sb.append(rel + " ");
		}

		LogicalPlan inner = op.getInnerPlan();
		
		StringBuilder innerSB = new StringBuilder();
		
		if (inner != null) {
			
			InnerPlanQueryGen gen = new InnerPlanQueryGen(inner, new DependencyOrderWalker(inner), innerSB );
			gen.visit();
			
			List<Operator> leaves = inner.getSinks();
			for (Operator leaf : leaves) {
				if (leaf instanceof LOGenerate) {
					String gen_stmt = visit( (LOGenerate) leaf);
					innerSB.append(gen_stmt + ";");
				}
			}
		}

		int innerLines = innerSB.toString().split(";").length;
		if (innerLines > 1) {
			sb.append("{ " + innerSB.toString() + " };");
		} else {
			sb.append( innerSB );
		}
		
		return sb.toString();
	}
	
	public static String visit(LOFilter filter) 
			throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String alias = filter.getAlias();
		sb.append(alias + " = FILTER "); //append the alias name to output script

		List<Operator> filterPred = filter.getPlan().getPredecessors(filter);
		for (Operator op : filterPred) {
			if (op instanceof LogicalRelationalOperator) {
				sb.append(((LogicalRelationalOperator)op).getAlias() + " ");
			}
		}

		sb.append("BY "); 
		//transform filter plan operations
		LogicalExpressionPlan filterPlan = filter.getFilterPlan();
		LogicalExpressionPlanResolver resolver = new LogicalExpressionPlanResolver(filterPlan);
		sb.append(resolver.getPlaintext());
		sb.append(";");
		return sb.toString();
	}

	public static String visit(LOJoin join) 
			throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String alias = join.getAlias();
		sb.append(alias+" = JOIN ");

		List<Operator> preds = join.getPlan().getPredecessors(join);
		List<String> aliases = new ArrayList<String>();
		for(Operator pred : preds){
			LogicalRelationalOperator lrop = (LogicalRelationalOperator)pred;
			aliases.add(lrop.getAlias());
		}

		LogicalSchema ls = join.getSchema();
		MultiMap<Integer, LogicalExpressionPlan> exprPlans = join.getExpressionPlans();
		int size = exprPlans.keySet().size();
		int i = 1;
		for(int key : exprPlans.keySet()){
			sb.append(aliases.get(i-1)+" BY ");
			List<LogicalExpressionPlan> plans = exprPlans.get(key);
			int listSize = plans.size();
			int num = 1;
			if(listSize > 1){
				sb.append("(");
			}

			for(LogicalExpressionPlan lep : plans){
				Iterator<Operator> ops = lep.getOperators();
				for(Iterator<?> it = ops; it.hasNext();){
					ProjectExpression pe = (ProjectExpression)it.next();
					if (pe.getColAlias() != null)
                        sb.append(pe.getColAlias());
                    else {
                        sb.append(pe.findReferent().getSchema().getField(pe.getColNum()).alias);
                    }
				}

				if(num < listSize){
					sb.append(", ");
				}
				num++;
			}
			if(listSize >1){
				sb.append(")");
			}

			if(i < size){
				sb.append(",");
			}
			i++;
		}

		//find out the join type
		JOINTYPE type = join.getJoinType();
		String typeStr = parseJoinType(type);
		if(typeStr !=null)
			sb.append(" USING "+typeStr);

		String partitioner = join.getCustomPartitioner();
		if(partitioner != null)
			sb.append(" PARTITION BY "+partitioner);

        sb.append(addParallel(join));
		sb.append(";");
		return sb.toString();
	}

	private static String parseJoinType(JOINTYPE type) {
		StringBuilder sb = new StringBuilder();
		sb.append("'");
		switch(type){
		case HASH:
			sb.append("hash");
			break;
		case MERGE:
			sb.append("merge");
			break;
		case MERGESPARSE:
			sb.append("merge-sparse");
			break;
		case REPLICATED:
			sb.append("replicated");
			break;
		case SKEWED:
			sb.append("skewed");
			break;
		default:
			break;

		}
		sb.append("'");
		return sb.toString();
	}

	public static String visit(LOGenerate gen)
			throws FrontendException {
		StringBuilder sb = new StringBuilder();
		sb.append("GENERATE ");

		boolean[] flattenFlags = gen.getFlattenFlags();

		List<LogicalExpressionPlan> out = gen.getOutputPlans();
		List<LogicalSchema> outSchemas = gen.getOutputPlanSchemas();

		for (int i = 0; i < out.size(); i++){
			// each iteration corresponds to an output field
			if (i > 0) sb.append(", ");
			
			if (flattenFlags[i])
				sb.append("flatten(");
			
			LogicalExpressionPlan plan = out.get(i);
			LogicalExpressionPlanResolver resolver = 
					new LogicalExpressionPlanResolver(plan);
			sb.append( resolver.getPlaintext() );

			LogicalSchema schema = outSchemas.get(i);
			List<LogicalFieldSchema> fields = schema.getFields();
			
			if ( fields.size() > 0 && !flattenFlags[i] ) {
				if (fields.size() > 1) {
                        sb.append("(");
				}

				for (int j = 0; j < fields.size(); j++) {
                    String fieldAlias = fields.get(j).alias;
					if (fieldAlias != null) {
						if (j > 0) sb.append(", ");

                        if (fieldAlias != null && !fieldAlias.equals("group")) {
                            sb.append(" AS " + fields.get(j).alias);
                        }

					}
				}

				if (fields.size() > 1) {
					sb.append(")");
				}
			}

			if ( fields.size() > 0 && flattenFlags[i] ) {
				sb.append(")");
                String fieldAlias = fields.get(0).alias;
                if (fieldAlias != null && !fieldAlias.startsWith("group")) {
                    sb.append(" AS " + fieldAlias);
                }
			}
		}  
		return sb.toString();
	}
	
	public static String visit(LOStore store) 
			throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String signature = store.getSignature();

		//retrieve the input alias
        String inputAlias = ((LogicalRelationalOperator)store.getPlan().getPredecessors(store).get(0)).getAlias();

		//retrieve the output file name
		String output = store.getOutputSpec().getFileName();

		sb.append("STORE "+inputAlias+" INTO \'"+output+"\'");
		FileSpec fileSpec = store.getFileSpec();
		FuncSpec funcSpec = fileSpec.getFuncSpec();
		if(funcSpec!=null){
			sb.append(" USING "+parseFuncSpec(funcSpec));
		}
		sb.append(";");
		return sb.toString();
	}
	
	
	public static String visit(LOCogroup group) 
			throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String alias = group.getAlias();
        if (group.getPlan().getPredecessors(group).size() > 1)
            sb.append(alias + " = COGROUP ");
		else
            sb.append(alias + " = GROUP ");

		LogicalSchema ls = group.getSchema();
		MultiMap<Integer, LogicalExpressionPlan> exprPlans = group.getExpressionPlans();
		int size = exprPlans.keySet().size();
		int i = 1;
		for(int key : exprPlans.keySet()){
			sb.append(ls.getField(key+1).alias + " ");
			List<LogicalExpressionPlan> plans = exprPlans.get(key);
			int listSize = plans.size();

			if(listSize == 1){
				LogicalExpressionPlan lep = plans.get(0);
				if(lep.size() == 1){
					//all option
					Operator allOP = lep.getOperators().next();
					if(allOP instanceof ConstantExpression){}
					else{
						sb.append(" BY ");
					}
				}
			}else{
				sb.append(" BY ");
			}
			int num = 1;
			if(listSize > 1){
				sb.append("(");
			}

			for(LogicalExpressionPlan lep : plans){
				LogicalExpressionPlanResolver resolver = 
						new LogicalExpressionPlanResolver(lep);
				String planStr = resolver.getPlaintext();

				if(planStr.equals("'all'")){
					planStr = "all";
				}
				sb.append(planStr);

				if(num < listSize){
					sb.append(",");
				}
				num++;
			}
			if(listSize >1){
				sb.append(")");
			}

			if(i < size){
				sb.append(",");
			}
			i++;
		}

		//resolve group type
		GROUPTYPE type = group.getGroupType();
		String typeStr = parseGroupType(type);
		if(typeStr != null){
			sb.append(" USING "+typeStr);
		}

		if(group.getCustomPartitioner()!=null){
			sb.append(" PARTITION BY "+group.getCustomPartitioner());
		}

        sb.append(addParallel(group));
		sb.append(";");
		return sb.toString();
	}
	
	private static String visit(LOCross cross) {
		StringBuilder sb = new StringBuilder();
		
		String alias = cross.getAlias();
		sb.append(alias + " = CROSS ");
		List<Operator> preds = cross.getPlan().getPredecessors(cross);
		int i = 0;
		for (Operator pred: preds) {
			if (i > 0) sb.append(", ");
			if (pred instanceof LogicalRelationalOperator) {
				sb.append( ((LogicalRelationalOperator) pred).getAlias() );
			}
			i++;
		}

		if (cross.getCustomPartitioner()!=null) {
			sb.append(" PARTITION BY "+cross.getCustomPartitioner());
		}

        sb.append(addParallel(cross));
		sb.append(";");

		return sb.toString();
	}

	private static String visit(LODistinct op) {
		StringBuilder sb = new StringBuilder();
		String alias = op.getAlias();
 	   if(alias != null){
 		   sb.append(alias+" = ");
 	   }
 	   sb.append("DISTINCT ");
 	   List<Operator> preds = op.getPlan().getPredecessors(op);
 	   for(Operator pred : preds){
 		   String pAlias = ((LogicalRelationalOperator) pred).getAlias();
 		   if(pAlias!=null)
 			   sb.append(pAlias+" ");
 	   }
 	   
 	   String partitioner = op.getCustomPartitioner();
 	   if(partitioner!=null){
 		   sb.append("PARTITION BY "+partitioner);
 	   }

       sb.append(addParallel(op));
 	   sb.append(";");
		return sb.toString();
	}
	
	private static String visit(LOLimit limit) {
		StringBuilder sb = new StringBuilder();
		String alias = limit.getAlias();
		sb.append(alias + " = LIMIT ");
		List<Operator> preds = limit.getPlan().getPredecessors(limit);
		int i = 0;
		for (Operator pred: preds) {
			if(i > 0) sb.append(", ");
			if(pred instanceof LogicalRelationalOperator){
				sb.append(((LogicalRelationalOperator)pred).getAlias());
			}
			i++;
		}
		LogicalExpressionPlan plan = limit.getLimitPlan();
		if(plan == null){
			long limitNum = limit.getLimit();
			sb.append(" "+limitNum);
		}else{
			LogicalExpressionPlanResolver resolver = 
					new LogicalExpressionPlanResolver(plan);
			sb.append(" "+resolver.getPlaintext());
		}
		sb.append(";");
		return sb.toString();
	}
	
	private static String visit(LOSort op) 
		throws FrontendException {
		StringBuilder sb = new StringBuilder();
		String alias = op.getAlias();
		sb.append(alias + " = ORDER ");

		List<Operator> preds = op.getPlan().getPredecessors(op);
		for (Operator pred : preds) {
			sb.append( ((LogicalRelationalOperator)pred).getAlias() + " ");
		}

		sb.append("BY ");

		List<LogicalExpressionPlan> plans = op.getSortColPlans();

		List<Boolean> ascCols = op.getAscendingCols();
		int i = 0;
		for(LogicalExpressionPlan plan : plans){
			if(i!=0){
				sb.append(",");
			}
			if(getPlanString(plan)!=null)
				sb.append(getPlanString(plan));
			if(!ascCols.get(i))
				sb.append(" DESC");
			i++;
		}

		sb.append(addParallel(op));
		sb.append(";");
		return sb.toString();
	}

    private static String addParallel(LogicalRelationalOperator op) {
        int rPP = op.getRequestedParallelism();
        if (rPP > 1)
            return " PARALLEL " + rPP;
        else
            return "";
    }

	private static String visit(LOSplit op) {
		StringBuilder sb = new StringBuilder();
		sb.append("SPLIT ");

		//there should be only one predecessor for each split
		//so that the following code should be okay
		List<Operator> preds = op.getPlan().getPredecessors(op);
		for(Operator pred : preds){
			String alias = ((LogicalRelationalOperator)op).getAlias();
			if ( alias != null) {
				sb.append(alias+ " INTO ");
			}
		}
		//now we need to retrieve all the splitOutput
		List<Operator> succs = op.getPlan().getSuccessors(op);
		int i = 0;
		for(Operator succ : succs){
			if(succ instanceof LOSplitOutput){
				//TODO: we need to find a better way to solve 
				//"Otherwise" keyword output
				//For now, it should work find but doing dirty work
				String opAlias = ((LogicalRelationalOperator)op).getAlias();
				if (i > 0) sb.append(", ");
				sb.append(opAlias+ " IF ");
				LogicalExpressionPlan plan = ((LOSplitOutput) succ).getFilterPlan();
				LogicalExpressionPlanResolver resolver = 
						new LogicalExpressionPlanResolver(plan);
				String planString = resolver.getPlaintext();
				sb.append(planString);
				i++;
			}
		}
		sb.append(";");
		return sb.toString();
	}
	
	private static String visit(LOUnion lou) {
		StringBuilder sb = new StringBuilder();
		String alias = lou.getAlias();
		sb.append(alias+" = UNION ");
		if (lou.isOnSchema()) {
			sb.append("ONSCHEMA ");
		}

		List<Operator> preds = lou.getPlan().getPredecessors(lou);

		for (Operator pred : preds) {
			String opAlias = ((LogicalRelationalOperator) pred).getAlias();
			if(opAlias != null) {
				sb.append(opAlias);
				if (pred != preds.get(preds.size() - 1))
					sb.append(",");
			}
		}

		sb.append(";");
		return sb.toString();
	}
	
	private static String getPlanString(LogicalExpressionPlan plan) throws FrontendException{
		LogicalExpression lgExpr = (LogicalExpression)plan.getSources().get(0);
		String colAlias = lgExpr.getFieldSchema().alias;
		return colAlias;
	}
	
	public static String visit(Operator op)
		throws FrontendException {
		
		if (op instanceof LOLoad) 
			return visit( (LOLoad) op );
		else if (op instanceof LOFilter)
			return visit( (LOFilter) op );
		else if (op instanceof LOForEach) 
			return visit( (LOForEach) op );
		else if (op instanceof LOGenerate)
			return visit( (LOGenerate) op );
		else if (op instanceof LOJoin)
			return visit( (LOJoin) op );
		else if (op instanceof LOCogroup)
			return visit( (LOCogroup) op );
		else if (op instanceof LOStore)
			return visit( (LOStore) op );
		else if (op instanceof LOCross)
			return visit( (LOCross) op );
		else if (op instanceof LOLimit)
			return visit( (LOLimit) op );
		else if (op instanceof LODistinct)
			return visit( (LODistinct) op );
		else if (op instanceof LOSort)
			return visit( (LOSort) op );
		else if (op instanceof LOSplit) 
			return visit( (LOSplit) op );
		else if (op instanceof LOUnion)
			return visit( (LOUnion) op );
		/*
		else if (op instanceof LOStream)
			return visit( (LOStream) op);
		*/
		else {
			LOG.error("Operator Not supported: " + op.getClass().getName());
			return null;
		}
	}
}
