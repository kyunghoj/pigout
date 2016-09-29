package edu.buffalo.cse.pigout.parser;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UnaryExpression;

public class LogicalExpressionPlanResolver {
	
	private LogicalExpressionPlan plan_;
	private StringBuilder plansb = new StringBuilder();

	public LogicalExpressionPlanResolver(LogicalExpressionPlan plan) {
		this.plan_ = plan;
		resolve();
	}
	
	public String getPlaintext(){
		return plansb.toString();
	}
	
	public void resolve(){
		List<Operator> sources = plan_.getSources();
	 	   for(Operator source : sources){
 			  if(source instanceof LogicalExpression){
 				 LogicalExpressionResolver resolver = 
							new LogicalExpressionResolver((LogicalExpression)source);
 				 plansb.append(resolver.getPlaintext());
 			  }
 		   }
	}
	
}
