package edu.buffalo.cse.pigout.partitioner;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.*;

public class PigScriptGenerator {
    PigOutLogicalChunks chunk;
    StringBuilder queryBuilder;


    public PigScriptGenerator(PigOutLogicalChunks chunk) throws FrontendException {
        this.chunk = chunk;
        this.queryBuilder = new StringBuilder();
    }

    public String getQueryScript() throws FrontendException {
        LogicalPlan lp = chunk.getPlan();
        PlanWalker walker = new DependencyOrderWalker( lp );
        new visitor(lp, walker, this.queryBuilder).visit();

        return this.queryBuilder.toString();
    }

    class visitor extends LogicalRelationalNodesVisitor {
        StringBuilder queryStringBuilder;

        protected visitor (OperatorPlan plan, PlanWalker walker, StringBuilder sb)
                throws FrontendException {
            super(plan, walker);
            this.queryStringBuilder = sb;
        }

        public void visit(LOLoad op) throws FrontendException {
            queryStringBuilder.append(LogicalToQuery.visit(op));
            queryStringBuilder.append("\n");
        }

        public void visit(LOStore op) throws FrontendException {
            queryStringBuilder.append(LogicalToQuery.visit(op));
            queryStringBuilder.append("\n");
        }

        public void visit(LOFilter op) throws FrontendException {
            queryStringBuilder.append(LogicalToQuery.visit(op));
            queryStringBuilder.append("\n");
        }

        public void visit(LOJoin op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LOCogroup op) throws FrontendException {
            queryStringBuilder.append(LogicalToQuery.visit(op));
            queryStringBuilder.append("\n");
        }

        public void visit(LOCross op)  throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LODistinct op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LOLimit op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LOSort op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LOSplit op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }

        public void visit(LOUnion op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op) );
            queryStringBuilder.append("\n");
        }
         public void visit(LOForEach op) throws FrontendException {
            queryStringBuilder.append( LogicalToQuery.visit(op));
            queryStringBuilder.append("\n");
        }
    }
}