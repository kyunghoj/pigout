package edu.buffalo.cse.pigout.parser;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class LogicalExpressionResolver {

    protected final Log log = LogFactory.getLog(getClass());

	private LogicalExpression lexpr;
	private StringBuilder sb = new StringBuilder();
	
	public LogicalExpressionResolver(LogicalExpression expr) {
		lexpr = expr;
		try {
			transform();
		} catch (FrontendException e) {
			e.printStackTrace();
		}
	}
	
	public String getPlaintext(){
		return sb.toString();
	}
/*
    private static String translate(LogicalFieldSchema lfs) {
        String uidString = "";
        byte type;
        LogicalSchema schema;

        String aliasToPrint = "";
        if (alias!=null)
            aliasToPrint = alias;

        type = lfs.type;

        String castType = DataType.findTypeName(lfs.type);
            if( type == DataType.BAG ) {
                if( schema == null ) {
                    return ( aliasToPrint + uidString + ":bag{}" );
                }
                return ( aliasToPrint + uidString + ":bag{" + schema.toString(verbose) + "}" );
            } else if( type == DataType.TUPLE ) {
                if( schema == null ) {
                    return ( aliasToPrint + uidString + ":tuple()" );
                }
                return ( aliasToPrint + uidString + ":tuple(" + schema.toString(verbose) + ")" );
            } else if (type == DataType.MAP) {
                if (schema == null ) {
                    return (aliasToPrint + uidString + ":map");
                } else {
                    return (aliasToPrint + uidString + ":map(" + schema.toString(verbose) + ")");
                }
            }
            return ( aliasToPrint + uidString + ":" + DataType.findTypeName(type) );
        }
    }
    */
	private void transform() throws FrontendException {
		if (lexpr instanceof EqualExpression) {
			EqualExpression ee = (EqualExpression)lexpr;
			LogicalExpression lhs = ee.getLhs();
			LogicalExpression rhs = ee.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append(ller.getPlaintext());
			sb.append(" == ");
			sb.append(rler.getPlaintext());
		} else if (lexpr instanceof NotEqualExpression) {
			NotEqualExpression ee = (NotEqualExpression)lexpr;
			LogicalExpression lhs = ee.getLhs();
			LogicalExpression rhs = ee.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append(ller.getPlaintext());
			sb.append(" != ");
			sb.append(rler.getPlaintext());
		} else if (lexpr instanceof ConstantExpression) {
			ConstantExpression ce = (ConstantExpression)lexpr;
			Object value = ce.getValue();
			if (value instanceof String) {
				sb.append("'" + value.toString() + "'");
			} else if (value instanceof Integer) {
				sb.append(value.toString());
			}
		} else if (lexpr instanceof ProjectExpression) {
			ProjectExpression pe = (ProjectExpression)lexpr;
            String colAlias = pe.getColAlias();

            LogicalSchema.LogicalFieldSchema lfs = pe.getFieldSchema();

            colAlias = lfs.alias;

            //String peStr = pe.toString();
            //String colAlias = peStr.split(":")[0];

            //if (colAlias != null && !colAlias.startsWith("(")) {
            if (colAlias != null) {
                sb.append(colAlias);
            } else if (pe.getColNum() < 0) {
                LogicalRelationalOperator lro = pe.getAttachedRelationalOp();
                if (lro instanceof LOGenerate) {
                    sb.append("$" + pe.getInputNum());
                }
            } else {
                sb.append("$" + pe.getColNum());
            }
		} else if (lexpr instanceof AndExpression) {
			AndExpression ae = (AndExpression)lexpr;

			LogicalExpression lhs = ae.getLhs();
			LogicalExpression rhs = ae.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append("(" + lsb + " and " + rsb + ")");
		} else if (lexpr instanceof OrExpression) {
			OrExpression ae = (OrExpression)lexpr;

			LogicalExpression lhs = ae.getLhs();
			LogicalExpression rhs = ae.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append("(" + lsb + " or " + rsb + ")");
		} else if (lexpr instanceof LessThanEqualExpression) {
			LessThanEqualExpression lte = (LessThanEqualExpression)lexpr;
			LogicalExpression lhs = lte.getLhs();
			LogicalExpression rhs = lte.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append(lsb+" <= "+rsb);
		} else if (lexpr instanceof LessThanExpression) {
			LessThanExpression lte = (LessThanExpression)lexpr;
			LogicalExpression lhs = lte.getLhs();
			LogicalExpression rhs = lte.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append(lsb + " < " + rsb);
		} else if (lexpr instanceof GreaterThanEqualExpression) {
			GreaterThanEqualExpression gte = (GreaterThanEqualExpression)lexpr;
			LogicalExpression lhs = gte.getLhs();
			LogicalExpression rhs = gte.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append(lsb + " >= " + rsb);
		} else if (lexpr instanceof GreaterThanExpression) {
			GreaterThanExpression gte = (GreaterThanExpression)lexpr;
			LogicalExpression lhs = gte.getLhs();
			LogicalExpression rhs = gte.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			String lsb = ller.getPlaintext();
			String rsb = rler.getPlaintext();
 			sb.append(lsb + " > " + rsb);
		} else if(lexpr instanceof NotExpression) {
			NotExpression notExpr = (NotExpression)lexpr;
			LogicalExpression le = notExpr.getExpression();
			LogicalExpressionResolver ler = 
					new LogicalExpressionResolver(le);
			sb.append("NOT(" + ler.getPlaintext() + ")");
		} else if (lexpr instanceof IsNullExpression) {
			IsNullExpression nullExpr = (IsNullExpression)lexpr;
			LogicalExpression expr = nullExpr.getExpression();
			LogicalExpressionResolver ler = 
					new LogicalExpressionResolver(expr);
			sb.append(ler.getPlaintext()+" is null");
		} else if (lexpr instanceof UserFuncExpression) {
			UserFuncExpression ufe = (UserFuncExpression)lexpr;
			String ufeString = visit(ufe);
			sb.append(ufeString);
		} else if (lexpr instanceof DereferenceExpression) {
            DereferenceExpression dfe = (DereferenceExpression) lexpr;
            String alias = dfe.getReferredExpression().getFieldSchema().alias;
            if (alias != null) {
                sb.append(alias);
            }
            String cols = dfe.getRawColumns().toString();
            if (cols != null) {
                sb.append(".");
                int indexOfEnd = cols.indexOf(']');
                cols = cols.substring(1, indexOfEnd);
                /*
                 *  02/25/2014 kyunghoj: There might be cases when we need $ sign
                 *  But for PigMix, I don't see such cases and adding $ creates a problem
                 *  when cols is not a column index.
                 *
                 *  4/18/2014 kyunghoj: If cols is "Number", then we need to add '$'.
                 */
                try {
                    sb.append("$" + Integer.parseInt(cols));
                } catch (NumberFormatException nfe) {
                    sb.append(cols);
                }
            }
        } else if (lexpr instanceof MapLookupExpression) {
            MapLookupExpression mapLookupExpr = (MapLookupExpression) lexpr;
            String mapExprStr = mapLookupExpr.getMap().toString();

            sb.append(mapExprStr.split(":")[0] + "#" + "'" + mapLookupExpr.getLookupKey() + "'");

        } else if (lexpr instanceof BinCondExpression) {
            BinCondExpression binCond = (BinCondExpression) lexpr;
            LogicalExpression condExpr = binCond.getCondition();

            LogicalExpression lhsExpr = binCond.getLhs();
            LogicalExpression rhsExpr = binCond.getRhs();

            LogicalExpressionResolver cond = new LogicalExpressionResolver(condExpr);
            LogicalExpressionResolver lhs = new LogicalExpressionResolver(lhsExpr);
            LogicalExpressionResolver rhs = new LogicalExpressionResolver(rhsExpr);

            sb.append("( " + cond.getPlaintext() + " ? " + lhs.getPlaintext() + " : " + rhs.getPlaintext() + " )");
        } else if (lexpr instanceof CastExpression) {
			CastExpression cast = (CastExpression)lexpr;

			LogicalFieldSchema lfs = cast.getFieldSchema();
            //System.out.println("CastExpression: " + lfs.toString(false));
            String castExprStr = lfs.toString(false);
            String [] castExprArry = castExprStr.split(":");

            castExprStr.replaceAll(":", "");

            StringBuilder castSB = new StringBuilder("(");

            for (int i = 1; i < castExprArry.length; i++) {
                String castExpr = castExprArry[i];
                String newCastExpr = castExpr.replace("map", "map[]");
                castSB.append(newCastExpr);
            }
            castSB.append(")");
            castSB.append(castExprArry[0]);
            sb.append(castSB);

            //System.out.println("CastExpression: " + castSB.toString());

            /* commented out on 7/9/2014 by kyunghoj */
            /*
            String castType = DataType.findTypeName(lfs.type);

            // expr: expression that this cast expr is projecting
            LogicalExpression expr = cast.getExpression();

            LogicalExpressionResolver ler = new LogicalExpressionResolver(expr);

            sb.append("(" + castType + ")" + ler.getPlaintext());
            */
            /*
            if (expr instanceof ProjectExpression) {
                ProjectExpression pe = (ProjectExpression)expr;

                if (pe.getColAlias() != null) {
                    sb.append("(" + castType + ")" + "$" + pe.getColAlias());
                } else {
                    sb.append("(" + castType + ")" + "$" + pe.getInputNum());
                }
            } else {
                // exception?
            }
            */

		} else if (lexpr instanceof DivideExpression) {
			DivideExpression divide = (DivideExpression)lexpr;
			LogicalExpression lhs = divide.getLhs();
			LogicalExpression rhs = divide.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append(ller.getPlaintext());
			sb.append("/");
			sb.append(rler.getPlaintext());
		} else if (lexpr instanceof MultiplyExpression) {
			MultiplyExpression multi = (MultiplyExpression)lexpr;
			LogicalExpression lhs = multi.getLhs();
			LogicalExpression rhs = multi.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append(ller.getPlaintext());
			sb.append(" * ");
			sb.append(rler.getPlaintext());
		} else if (lexpr instanceof SubtractExpression) {
			SubtractExpression sub = (SubtractExpression)lexpr;
			LogicalExpression lhs = sub.getLhs();
			LogicalExpression rhs = sub.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append("("+ller.getPlaintext());
			sb.append(" - ");
			sb.append(rler.getPlaintext()+")");
		} else if(lexpr instanceof AddExpression) {
			AddExpression add = (AddExpression)lexpr;
			LogicalExpression lhs = add.getLhs();
			LogicalExpression rhs = add.getRhs();
			
			LogicalExpressionResolver ller = 
					new LogicalExpressionResolver(lhs);
			LogicalExpressionResolver rler =
					new LogicalExpressionResolver(rhs);
			
			sb.append("("+ller.getPlaintext());
			sb.append(" + ");
			sb.append(rler.getPlaintext()+")");
		}
		
	}

	private String visit(UserFuncExpression ufe) throws FrontendException {
		StringBuilder sb = new StringBuilder();
		sb.append(ufe.getFuncSpec().toString()+"(");
		List<LogicalExpression> args = ufe.getArguments();
		int i = 0;
		for(LogicalExpression le : args){
			if(i>0) sb.append(",");
			LogicalExpressionResolver resolver = 
					new LogicalExpressionResolver(le);
			sb.append(resolver.getPlaintext());
			i++;
		}
		sb.append(")");
		return sb.toString();
	}

}
