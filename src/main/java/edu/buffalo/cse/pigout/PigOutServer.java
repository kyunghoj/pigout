/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.buffalo.cse.pigout;

import edu.buffalo.cse.pigout.parser.QueryParserDriver;
import edu.buffalo.cse.pigout.partitioner.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.UriUtil;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import edu.buffalo.cse.pigout.partitioner.DepthFromSources;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * A class for Java programs to connect to PigOut. 
 *
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class PigOutServer {
    protected final Log log = LogFactory.getLog(getClass());

    public static final String PRETTY_PRINT_SCHEMA_PROPERTY 
        = "pigout.pretty.print.schema";
    private static final String PIGOUT_LOCATION_CHECK_STRICT 
        = "pigout.location.check.strict";


    /*
     * The data structure to support pigout shell (PigOutSh) operations.
     * The shell can only work on one graph at a time.
     * If a script is contained inside another script, the PigOut
     * shell first saves the current graph on the stack and works
     * on a new graph. After the nested script is done, the PigOut
     * shell pops up the saved graph and continues working on it.
     */
    private final Deque<Graph> graphs = new LinkedList<Graph>();

    /*
     * The current Graph the shell is working on
     */
    private Graph currDAG;

    protected final PigContext pigContext;

    private String jobName;

    private String jobPriority;

    private final static AtomicInteger scopeCounter = new AtomicInteger(0);

    protected final String scope = constructScope();

    private boolean isMultiQuery = true;
    private boolean aggregateWarning = true;

    private boolean validateEachStatement = false;
    private boolean skipParseInRegisterForBatch = false;

    private String constructScope() {
        // scope servers for now as a session id

        // String user = System.getProperty("user.name", "DEFAULT_USER_ID");
        // String date = (new Date()).toString();

        // scope is not really used in the system right now. It will
        // however make your explain statements look lengthy if set to
        // username-date. For now let's simplify the scope, if a real
        // scope is needed again, we might need to update all the
        // operators to not include scope in their name().
        return "" + scopeCounter.incrementAndGet();
    }

    public PigOutServer(PigContext context) throws ExecException {
        this.pigContext = context;
        currDAG = new Graph(false);
        this.pigContext.connect();
    }

    public void setBatchOn() {
        log.debug("Create a new graph.");

        if (currDAG != null) {
            graphs.push(currDAG);
        }
        currDAG = new Graph(isMultiQuery);
    }

    public void registerQuery(String query, int startLine) throws IOException {
        currDAG.registerQuery(query, startLine, 
                validateEachStatement, skipParseInRegisterForBatch);
    }

    /**
     * Set whether to skip parsing while registering the query in batch mode
     * @param skipParseInRegisterForBatch
     */
    public void setSkipParseInRegisterForBatch(boolean skipParseInRegisterForBatch) {
        this.skipParseInRegisterForBatch = skipParseInRegisterForBatch;
    }

    protected class Graph {

        private final Map<LogicalRelationalOperator, LogicalPlan> aliases
            = new HashMap<LogicalRelationalOperator, LogicalPlan>();

        private Map<String, Operator> operators = new HashMap<String, Operator>();
        private String lastRel;

        private final List<String> scriptCache = new ArrayList<String>();

        private Map<String, String> fileNameMap = new HashMap<String, String>();

        private final boolean batchMode;

        private int processedStores = 0;

        private LogicalPlan lp;

        private int currentLineNum = 0;

        public String getLastRel() {
            return lastRel;
        }

        public Graph(boolean batchMode) {
            this.batchMode = batchMode;
            this.lp = new LogicalPlan();
            log.debug("batchMode: " + this.batchMode);
        };

        private void countExecutedStores() {
            for( Operator sink : lp.getSinks() ) {
                if( sink instanceof LOStore ) {
                    processedStores++;
                }
            }
        }

        Map<LogicalRelationalOperator, LogicalPlan> getAliases() {
            return aliases;
        }

        Map<String, Operator> getAliasOp() {
            return operators;
        }

        boolean isBatchOn() {
            return batchMode;
        };

        boolean isBatchEmpty() {
            for( Operator op : lp.getSinks() ) {
                if( op instanceof LOStore )
                    return false;
            }
            return true;
        }

        void markAsExecuted() {
        }

        public LogicalPlan getLogicalPlan() {
            return this.lp;
        }

        /**
         * Get the operator with the given alias in the raw plan. Null if not
         * found.
         */
        Operator getOperator(String alias) throws FrontendException {
            return operators.get( alias );
        }

        public LogicalPlan getPlan(String alias) throws IOException {
            LogicalPlan plan = lp;

            if (alias != null) {
                LogicalRelationalOperator op = (LogicalRelationalOperator) operators.get(alias);
                if(op == null) {
                    int errCode = 1003;
                    String msg = "Unable to find an operator for alias " + alias;
                    throw new FrontendException(msg, errCode, PigException.INPUT);
                }
                plan = aliases.get(op);
            }
            return plan;
        }

        /**
         * Build a plan for the given alias. Extra branches and child branch under alias
         * will be ignored. Dependent branch (i.e. scalar) will be kept.
         * @throws IOException
         */
        void buildPlan(String alias) throws IOException {
            if( alias == null )
                skipStores();

            final Queue<Operator> queue = new LinkedList<Operator>();
            if( alias != null ) {
                Operator op = getOperator( alias );
                if (op == null) {
                    String msg = "Unable to find an operator for alias " + alias;
                    throw new FrontendException( msg, 1003, PigException.INPUT );
                }
                queue.add( op );
            } else {
                List<Operator> sinks = lp.getSinks();
                if( sinks != null ) {
                    for( Operator sink : sinks ) {
                        if( sink instanceof LOStore )
                            queue.add( sink );
                    }
                }
            }

            LogicalPlan plan = new LogicalPlan();

            while( !queue.isEmpty() ) {
                Operator currOp = queue.poll();
                plan.add( currOp );

                List<Operator> preds = lp.getPredecessors( currOp );
                if( preds != null ) {
                    List<Operator> ops = new ArrayList<Operator>( preds );
                    for( Operator pred : ops ) {
                        if( !queue.contains( pred ) )
                            queue.add( pred );
                        plan.connect( pred, currOp );
                    }
                }

                // visit expression associated with currOp. If it refers to any other operator
                // that operator is also going to be enqueued.
                currOp.accept( new AllExpressionVisitor( plan, new DependencyOrderWalker( plan ) ) {
                    @Override
                    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan exprPlan)
                            throws FrontendException {
                        return new LogicalExpressionVisitor( exprPlan, new DependencyOrderWalker( exprPlan ) ) {
                            @Override
                            public void visit(ScalarExpression expr) throws FrontendException {
                                Operator refOp = expr.getImplicitReferencedOperator();
                                if( !queue.contains( refOp ) )
                                    queue.add( refOp );
                            }
                        };
                    }
                }
                );

                currOp.setPlan( plan );
            }

            log.debug("=========== Before Pig Optimization ===========");
            log.debug(this.lp);


            Set<String> disabledOptimizerRules = new HashSet<String>();
            LogicalPlanOptimizer opt = new LogicalPlanOptimizer(plan, 100, disabledOptimizerRules);
            opt.optimize();


            this.lp = plan;
            log.debug("=========== After Pig Optimization ===========");
            log.debug(this.lp);

       }

        /**
         *  Remove stores that have been executed previously from the overall plan.
         */
        private void skipStores() throws IOException {
            List<Operator> sinks = lp.getSinks();
            List<Operator> sinksToRemove = new ArrayList<Operator>();
            int skipCount = processedStores;
            if( skipCount > 0 ) {
                for( Operator sink : sinks ) {
                    if( sink instanceof LOStore ) {
                        sinksToRemove.add( sink );
                        skipCount--;
                        if( skipCount == 0 )
                            break;
                    }
                }
            }

            for( Operator op : sinksToRemove ) {
                Operator pred = lp.getPredecessors( op ).get(0);
                lp.disconnect( pred, op );
                lp.remove( op );
            }
        }

        private int currentLinNum = 0;

        public void registerQuery(String query, int startLine,
                boolean validateEachStatement,
                boolean skipParseForBatch) throws IOException {

            if ( batchMode ) {
                // accumulate query strings to scriptCache and return
                if ( startLine == currentLinNum ) {
                    String line = scriptCache.remove( scriptCache.size() - 1 );
                    scriptCache.add( line + query );
                } else {
                    while ( startLine > currentLineNum + 1 ) {
                        scriptCache.add( "" );
                        currentLineNum++;
                    }
                    BufferedReader br = new BufferedReader(new StringReader(query));
                    String line = br.readLine();
                    while (line != null) {
                        scriptCache.add(line);
                        currentLineNum++;
                        line = br.readLine();
                    }
                }
                if (skipParseForBatch) {
                    return;
                }
            } else {
                // add query string to scriptCache
                scriptCache.add( query );
                log.debug("[debug] scriptCache: " + scriptCache);
            }

            if ( validateEachStatement ) {
                // validate
                validateQuery();
            }

            parseQuery();

            if ( !batchMode ) {
                buildPlan( null );
                for( Operator sink : lp.getSinks() ) {
                    if( sink instanceof LOStore ) {
                        try {
                            ;//execute();
                        } catch (Exception e) {
                            int errCode = 1002;
                            String msg = "Unable to store alias "
                                + ((LOStore) sink).getAlias();
                            throw new FrontendException(msg, errCode,
                                    PigException.INPUT, e);
                        }
                        break;
                    }
                }
            }
        }

        /**
         * Parse the accumulated pig statements and generate an overall plan.
         * @throws FrontendException
         */
        private void parseQuery() throws FrontendException {
            // TODO: Implement this!
            UDFContext.getUDFContext().reset();
            //UDFContext.getUDFContext().setClientSystemProps(pigContext.getProperties());

            String query = buildQuery();

            // obviously, return an empty logical plan if query is empty
            if ( query.isEmpty() ) {
                lp = new LogicalPlan();
                log.debug("empty logical plan returned.");
                return;
            }

            try {
                // Only need to get a logical plan...\
                log.debug("isMultiQuery? " + isMultiQuery);
                log.debug("isBatchOn? " + isBatchOn());
                QueryParserDriver parserDriver = new QueryParserDriver( pigContext, scope, fileNameMap );
                lp = parserDriver.parse( query );
                operators = parserDriver.getOperators();
                lastRel = parserDriver.getLastRel();

                if (lp == null)
                    log.error("parseQuery(): Parser returns a null LP.");

            } catch (Exception ex) {
                int errCode = 1000;
                scriptCache.remove( scriptCache.size() - 1 ); // remove the bad script from the cache
                //PigOutException pe = 
                String msg = "Error during parsing. " + ex.getMessage();
                log.error(msg, ex);
                throw new FrontendException (msg, errCode, PigException.INPUT, ex);
            }


        }

        private void validateQuery() {
            String query = buildQuery();
            QueryParserDriver parserDriver = new QueryParserDriver( pigContext, scope, fileNameMap );
            try {
                LogicalPlan plan = parserDriver.parse( query );
                compile( plan );
            } catch (FrontendException ex) {
                scriptCache.remove( scriptCache.size() - 1 );
            }
        }

        private void compile(LogicalPlan lp) throws FrontendException {
        }

        private String buildQuery() {
            StringBuilder accuQuery = new StringBuilder();
            for( String line : scriptCache ) {
                accuQuery.append( line + "\n" );
            }

            return accuQuery.toString();
        }

        private void compile() throws IOException {
            compile( lp );
            currDAG.postProcess();
        }

        private void postProcess() throws IOException {
            // TODO Auto-generated method stub

        }

        /**
         * This method checks whether the multiple sinks (STORE) use the same
         * "file-based" location. If yes, throws a RuntimeException
         * 
         * @param storeOps
         */
        private void checkDuplicateStoreLoc(Set<LOStore> storeOps) {
            Set<String> uniqueStoreLoc = new HashSet<String>();
            for(LOStore store : storeOps) {
                String fileName = store.getFileSpec().getFileName();
                if(!uniqueStoreLoc.add(fileName) && UriUtil.isHDFSFileOrLocalOrS3N(fileName)) {
                    throw new RuntimeException("Script contains 2 or more STORE statements writing to same location : "+ fileName);
                }
            }
        }

    }

    public boolean isBatchOn() {
        // Batch is on when there are multiple graphs on the stack
        return graphs.size() > 0;
    }

    public void parseAndBuild() throws IOException {
        if (currDAG == null || !isBatchOn()) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigOutException.INPUT);
        }

        currDAG.parseQuery();
        currDAG.buildPlan( null );

    }

    private Map<Operator, Collection<Operator>> slices;
    
    public void partitionBatch() throws IOException {

        // Error condition check
        if (currDAG == null || !isBatchOn()) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigOutException.INPUT);
        }
      
        // Cluster assignment
        Map<Operator, String> assignment = placeOperators();
        if (assignment == null) {
            log.error( "siteAssignment is null." );
            return;
        }
        
        PigOutPartitioner partitioner;
        partitioner =
            new PigOutPartitioner(this, assignment);

        List<PigOutLogicalChunks> chunkList = partitioner.createLogicalChunks();

        // Optimizer
        PigOutPartitionOptimizer pigout_opt = new PigOutPartitionOptimizer(chunkList);
        pigout_opt.optimize();

        // Generate coordinator app for each chunk
        List<PigOutLogicalChunks> optimizedChunks = pigout_opt.getChucks();
        for (PigOutLogicalChunks optChunk : optimizedChunks) {

            // Build a reverse dependency list
            List<PigOutLogicalChunks> succChunks = new ArrayList<>();
            for (PigOutLogicalChunks chunk : optimizedChunks) {
                if ( chunk.getPredecessors().contains(optChunk) ) {
                    succChunks.add( chunk );
                }
            }

            genPigScript( optChunk );
            genOozieCoord( optChunk );
            genOozieWorkflow( optChunk, succChunks );
        }
    }

    private void genPigScript( PigOutLogicalChunks chunk ) 
        throws FrontendException {

        Properties conf = this.getContext().getProperties();
        String prefix = conf.getProperty("pigout.oozie.local");
        String jobName = conf.getProperty(PigContext.JOB_NAME);
        String oozieDir = prefix + "/" + jobName + "/" + chunk.getAlias() + "/";

        String query = new PigScriptGenerator( chunk ).getQueryScript();
        log.debug("\n" + query);

        try {
            String pigScriptFile = chunk.getAlias() + ".pig";

            File file 
                = new File( oozieDir + "/" + pigScriptFile);

            if ( !file.getParentFile().exists() )
                file.getParentFile().mkdirs();
            Writer writer = new BufferedWriter(new FileWriter(file));
            writer.write(query);
            writer.close();
        } catch (IOException e) {
            log.error("Write Pig script to local filesystem failed.");
            e.printStackTrace();
        }
    }

    private void genOozieCoord( PigOutLogicalChunks chunk )
        throws FrontendException, IOException {

        Properties conf = this.getContext().getProperties();
        OozieCoordGen coord = new OozieCoordGen(chunk, conf);
        coord.generate();

        String prefix = conf.getProperty("pigout.oozie.local");
        String jobName = conf.getProperty(PigContext.JOB_NAME);
        String oozieDir = prefix + "/" + jobName + "/" + chunk.getAlias() + "/";

        coord.writeXMLDoc(oozieDir + "coordinator.xml");
        coord.writeProperties(oozieDir + "coordinator.properties");
    }

    private void genOozieWorkflow( PigOutLogicalChunks chunk, List<PigOutLogicalChunks> list )
        throws IOException {

        Properties conf = this.getContext().getProperties();
        OozieWorkflowGen workflowGen = new OozieWorkflowGen( chunk, list, conf );
        workflowGen.generate();

        // TODO:
        // if workflowGen.isCopyOnlyWorkflow == true,
        // we need to "clone" LoadFunc from the first step script to
        // the next step script, but how?
        String prefix = conf.getProperty("pigout.oozie.local");
        String jobName = conf.getProperty(PigContext.JOB_NAME);
        String oozieDir = prefix + "/" + jobName + "/" + chunk.getAlias() + "/";

        workflowGen.writeXMLDoc(oozieDir + "workflow.xml");

    }

    // TODO: Move partioning related methods and members to Graph
    // As we do partitioning on the current query plan graph
    private Map<Operator, List<String>> assignments;
    
    private void assignClusters() throws FrontendException {
        LogicalPlan lp = this.getCurrLogicalPlan();

        AssignClustersFromLoads asv = new AssignClustersFromLoads(lp);
        asv.visit();

        AssignClustersFromStores acs 
            = new AssignClustersFromStores(lp, asv.getMap());
        acs.visit();

        // set of every single possible assignment
        assignments = acs.getMap();
    }

    /**
     * Given a cost estimator, assign each operator to a cluster.
     */
    private Map<Operator, String> placeOperators() throws FrontendException {
   	
    	LogicalPlan lp = this.getCurrLogicalPlan();
   
        // Assign clusters for each operator
        assignClusters();

        // Compute min-distance from sources
        Map<Operator, Integer> depths = getDepthFromSources();

        // Estimate data transfer costs on the edges in the logical plan
        LogicalPlanCostEstimator estimator = new DataTransferCostEstimator( pigContext, lp );

    	estimator.visit();
    	Map<Operator, Float> costTable = estimator.getCostTable();
    	
        // Enumerate: transform Operator -> Set<String> into
        // Set<Map<Operator, String>>, essentially it becomes
        // a set of mappings. It means all the different ways 
        // of assigning a cluster to each operator.
        
        EnumerateAssignments ea = new EnumerateAssignments(lp, assignments);
        ea.visit();
        
    	Set<Map<Operator, String>> all = ea.get();

        // build a list of candidate assignments based on data transfer cost
        List<Map<Operator, String>> asstList = new ArrayList<>();

        Map<Operator, String> minAssignment = null;
    	float minCost = (float)0.0;
 
    	for (Map<Operator, String> as : all) {
    		float cost = costOfAssignment(costTable, as);
    		if (minAssignment == null) {
    			minAssignment = as;
    			minCost = cost;
                asstList.add( minAssignment );
    		} else if (cost <= minCost) {
    			minAssignment = as;
    			minCost = cost;
    		} else if (cost == minCost) {
                int d_as = minDepthOfAsst(depths, as);
                int d_min = minDepthOfAsst(depths, minAssignment);
                if (d_min < d_as)
                    minAssignment = as;
            }
    	}

        for (Map<Operator, String> as : asstList) {
            float cost = costOfAssignment(costTable, as);
            if (cost > minCost) continue;
            // Compute a cost associated with partitioning. 
            // for example, count the number of steps from data sources
            // contiguously assigned to a cluster
        }

    	log.debug("minCost: " + minCost);
    	log.debug("minAssignment: " + minAssignment);
    	
    	return minAssignment;
    }

    private Integer minDepthOfAsst(Map<Operator, Integer> depths, Map<Operator, String> as) 
        throws FrontendException {
        
        LogicalPlan lp = this.getCurrLogicalPlan();
        Set<Operator> operators = as.keySet();
        int min_depth = 9999;

        for (Operator op : operators) {

            List<Operator> preds = lp.getPredecessors(op);
            for (Operator p : preds) {
                String predSite = as.get(p);
                String currSite = as.get(op);
                if (predSite.equals(currSite)) continue;

                int d = depths.get(p);
                if (min_depth > d)
                    min_depth = d;
            }


        }

        return min_depth;
    }

    private Map<Operator, Integer> getDepthFromSources() throws FrontendException {
        LogicalPlan lp = this.getCurrLogicalPlan();
        
        DepthFromSources dpsv = new DepthFromSources(lp);
        dpsv.visit();

        return dpsv.getDepths();
    }

    // TODO: rename the method as this is only data transfer cost
    private Float costOfAssignment( Map<Operator, Float> costs, Map<Operator, String> assignment)
    {
    	LogicalPlan lp = this.getCurrLogicalPlan();
    	Iterator<Operator> it = lp.getOperators();
    	float cost = (float)0.0;
    	while (it.hasNext()) {
    		Operator op = it.next();
    		List<Operator> preds = lp.getPredecessors(op);
    		if (preds == null) continue;
    		for (Operator pred : preds) {
    			// if there's a cut
    			if (0 != assignment.get(pred).compareTo(assignment.get(op)) ) {
    				cost += costs.get(pred);
    			}
    		}
    	}
    	
    	return cost;
    }

    public String getLastRel() {
        return currDAG.getLastRel();
    }

    public PigContext getContext() {
        return pigContext;
    }

    public void discardBatch() throws FrontendException {
        if (currDAG == null || !isBatchOn()) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigOutException.INPUT);
        }

        currDAG = graphs.pop();
    }

    public LogicalPlan getCurrLogicalPlan() {
        return this.currDAG.getLogicalPlan();
    }

    private URL locateJarFromResources(String jarName) throws IOException {
        Enumeration<URL> urls = ClassLoader.getSystemResources(jarName);
        URL resourceLocation = null;

        if (urls.hasMoreElements()) {
            resourceLocation = urls.nextElement();
        }

        if (urls.hasMoreElements()) {
            StringBuffer sb = new StringBuffer("Found multiple resources that match ");
            sb.append(jarName);
            sb.append(": ");
            sb.append(resourceLocation);

            while (urls.hasMoreElements()) {
                sb.append(urls.nextElement());
                sb.append("; ");
            }

            log.debug(sb.toString());
        }

        return resourceLocation;
    }
    
    public void registerJar(String name) throws IOException {
    	if (pigContext.hasJar(name)) {
    		log.debug("Ignoring duplicate registration for jar " + name);
    		return;
    	}
    	
    	// first try to locate jar via system resources
    	// if this fails, try by using "name" as File (this preserves
    	// compatibility with case when user passes absolute path or path
    	// relative to current working directory.)
    	if (name != null) {
    		if (name.isEmpty()) {
    			log.warn("Empty string specified for jar path");
    			return;
    		}
    		
    		URL resource = locateJarFromResources(name);
    		
    		if (resource == null) {
    			FetchFileRet[] files = FileLocalizer.fetchFiles(pigContext.getProperties(), name);
    			for (FetchFileRet file : files) {
                    File f = file.file;
                    if (!f.canRead()) {
                        int errCode = 4002;
                        String msg = "Can't read jar file: " + name;
                        throw new FrontendException(msg, errCode, PigException.USER_ENVIRONMENT);
                    }

                    pigContext.addJar(f.toURI().toURL(), name);
    			}
    		} else {
    				pigContext.addJar(resource, name);
    		}
    	}
    }
}
