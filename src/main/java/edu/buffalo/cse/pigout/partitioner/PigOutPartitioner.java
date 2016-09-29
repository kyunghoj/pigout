package edu.buffalo.cse.pigout.partitioner;

import edu.buffalo.cse.pigout.PigOutServer;
import edu.buffalo.cse.pigout.tools.PigOutUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

public class PigOutPartitioner {

	private final Log log = LogFactory.getLog(getClass());
	private PigContext context;
	private LogicalPlan plan;
	
	// site assignments given by the caller. 
	// assignment must be determined before partitioning starts.
	public Map<Operator, String> siteAssignment; 

	// visit order of partitioner
	private Queue<Operator> visitQueue;

	public Map<Operator, Collection<Operator>> slices = new HashMap<>();

	public Map<Operator, Operator> seenFrom = new HashMap<>();

	// subQueries: (subtree's root node) -> query string
	private Map<Operator, String> subQueries = new HashMap<>();

	private Map<String, Set<Operator>> sources = new HashMap<>();

    /*
     * load_fmt/store_fmt is a template for injected LOAD/STORE statements
     */
    private static String load_fmt = "%s = LOAD '$%s' USING %s AS (%s);\n";
    private static String store_fmt = "STORE %s INTO '$%s';\n";

    public PigOutPartitioner(PigOutServer pigout, Map<Operator, String> siteAssignment )
					throws FrontendException {

		this.plan = pigout.getCurrLogicalPlan();
		this.context = pigout.getContext();
		this.siteAssignment = siteAssignment;

		this.visitQueue = new LinkedList<>();

        init_by_source();
	}

    private void init_by_source() throws FrontendException {
    	List<Operator> srcOps = this.plan.getSources();
    	for (Operator src : srcOps) {
    		Set<Operator> from = new HashSet<>();
    		from.add( src );
    		setVisitOrder(from);
    	}
    }
    
    private void init_by_site() throws FrontendException {
   		List<Operator> srcOps = this.plan.getSources();

		for (Operator src : srcOps) {
			try {
				LOLoad load = (LOLoad) src;
				
				org.apache.hadoop.fs.Path filePath =
						 new  org.apache.hadoop.fs.Path( load.getSchemaFile() );
				URI uri = filePath.toUri();
				String nameNodeAddr = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
				
                // Maintain data source sites
                // site -> set of load opertors
				if ( this.sources.get(nameNodeAddr) != null ) {
					this.sources.get(nameNodeAddr).add( src );
				} else {
					Set<Operator> srcSet = new HashSet<Operator>();
					srcSet.add( src );
					this.sources.put( nameNodeAddr, srcSet );
				}
				log.debug( "data source namenode: " + nameNodeAddr );
				
			} catch (Exception e) {
				throw new FrontendException(
                     "Source node is not a LOAD operator, or in an invalid form.");
			}
		}
	
		// for each data source site (let's say data owner),
		//   start from sources and merge the operators to a block
		for ( String owner : this.sources.keySet() ) {
			Set<Operator> from = this.sources.get( owner );
            setVisitOrder(from);
		}
    }

    /*
     * Set visit order of partition discovery (BFS traversal)
     * @param from a set of starting operators
     */
    private void setVisitOrder(Set<Operator> from) {
        if (from == null) return;

        Queue<Operator> fifo = new LinkedList<>(from);

        while (!fifo.isEmpty()) {
            Operator d = fifo.poll();
            visitQueue.add(d);

            List<Operator> successors = this.plan.getSuccessors(d);
            if (successors == null) continue;
            for (Operator succ : successors) {
                fifo.add(succ);
            }
        }
    }

    /*
     * Adding a new LOStore operator at the logical plan
     * @param   lp      logical plan
     * @param   op      where LOStore operator will be connected
     */
    private void addNewLOStore(LogicalPlan lp, Operator op) throws FrontendException {
        Properties conf = this.context.getProperties();

        FileSpec outputFS;
        StoreFuncInterface storeFunc = null;

        String tmpdir_fmt = "%s/workflows/%s/tmp/%s";
        String signature = null;

        LogicalRelationalOperator lro = (LogicalRelationalOperator)op;

        String fileName 
            = String.format(tmpdir_fmt, conf.get("pigout.hdfs.dir.prefix"),
                conf.get("jobName"), lro.getAlias());

        String func = context.getProperties().getProperty(PigConfiguration.PIG_DEFAULT_STORE_FUNC, PigStorage.class.getName());
        FuncSpec funcSpec = new FuncSpec( func );
        storeFunc = (StoreFuncInterface)PigContext.instantiateFuncFromSpec( funcSpec );
        outputFS = new FileSpec( fileName, funcSpec );

        LOStore store = new LOStore(lp, outputFS, storeFunc, signature);

        LogicalSchema schema = lro.getSchema();
        store.setSchema(schema);
        lp.add(store);
        lp.connect(op, store);
    }

    // TODO: schema and signature
    private void addNewLOLoad(LogicalPlan lp, Operator op) throws FrontendException {
        Properties conf = this.context.getProperties();

        FileSpec inputFS;
        LoadFunc loadFunc;
        String tmpdir_fmt = "%s/workflows/%s/tmp/%s";

        for (Operator pred : this.plan.getPredecessors(op)) {
            // Build a temp filename from predecessor's alias
            // the temp directory/file name is generated in a same rule with workflow generation
            LogicalRelationalOperator pred_lrop = (LogicalRelationalOperator) pred;
            String pred_alias = pred_lrop.getAlias();
            String fileName
                    = String.format(tmpdir_fmt, conf.get("pigout.hdfs.dir.prefix"),
                    conf.get("jobName"), pred_alias);

            String func;
            func = context.getProperties().getProperty(
                        PigConfiguration.PIG_DEFAULT_STORE_FUNC, PigStorage.class.getName());
            FuncSpec funcSpec = new FuncSpec( func );

            loadFunc = (LoadFunc) PigContext.instantiateFuncFromSpec( funcSpec );
            inputFS = new FileSpec( fileName, funcSpec );

            LogicalSchema schema = pred_lrop.getSchema();

            LogicalSchema newSchema = new LogicalSchema();

            // "group" cannot be used as an alias.
            // Thus, replace it in added LOAD statement
            // kyunghoj, 04/19/2014
            for (int i = 0; i < schema.size(); i++) {
                LogicalFieldSchema lfs = schema.getField(i);
                LogicalFieldSchema newLfs = lfs.deepCopy();
                if (newLfs.alias != null && newLfs.alias.equals("group")) {
                    newLfs.alias = "new_alias";
                } else if (newLfs.alias == null) {
                    newLfs.alias = "f" + i;
                }
                newSchema.addField(newLfs);
            }

            String signature = pred_alias + "_newOperatorKey";

            LOLoad load = new LOLoad( inputFS, newSchema, lp, new Configuration(), loadFunc, signature );
            load.setAlias(pred_alias);

            List<Integer> requiredFields = new LinkedList<>();
            for (int i = 0; i < newSchema.size(); i++)
                requiredFields.add( i );
            load.setRequiredFields(requiredFields);

            // force generation of schema
            load.getSchema();
            lp.add(load);
            lp.connect(load, op);
        }

    }

    private Map<LogicalPlan, List<Operator>> planSuccs = new HashMap<>();
    private Map<LogicalPlan, List<Operator>> planPreds = new HashMap<>();
    private Map<Operator, LogicalPlan> subPlans =
            new HashMap<>();

    private void discoverPartition(
            Operator currOp,
            Set<Operator> discovered, LogicalPlan newPlan) {

    	List<Operator> successors = this.plan.getSuccessors(currOp);
        if (successors == null) {
        	log.debug("discovery reaches a sink.");
        	return;
        }

        for (Operator succ : successors) {
            if ( needToCut( currOp, succ ) ) {
                // succ is a successor of newPlan
            	log.debug("Need to break: " + ((LogicalRelationalOperator)currOp).getAlias() + "--" + ((LogicalRelationalOperator)succ).getAlias());
                List<Operator> planSuccList;

                if (planSuccs.containsKey(newPlan)) {
                    planSuccList = planSuccs.get(newPlan);
                } else {
                    planSuccList = new LinkedList<>();
                    planSuccs.put(newPlan, planSuccList);
                }
                planSuccList.add(succ);
                continue;
            }

            if (discovered.add(succ)) {
                newPlan.add(succ);
                subPlans.put(succ, newPlan);

                newPlan.connect(currOp, succ);
                Collection<Operator> nexts = this.plan.getSuccessors(succ);
                discoverPartition(succ, discovered, newPlan);
            }
        }
    }

    private long computeDataInSize(LogicalPlan lp) {
        long dataIn = 0l;

        for (Operator src : lp.getSources()) {
            String nameNode = this.siteAssignment.get(src);
            String hostname = nameNode.split(":")[0];
            String portNum = this.context.getProperties().getProperty(hostname + ".job.tracker").split(":")[1];
            int port = new Integer(portNum);

            if (!(src instanceof LOLoad)) continue;
            LOLoad load = (LOLoad) src;
            dataIn =+ PigOutUtils.getHDFSFileSize(load.getFileSpec().getFileName(), nameNode);
        }
        return dataIn;
    }

    // TODO: how to get output data size?
    private long computeDataOutSize(LogicalPlan lp) {
        long dataOut = 0l;

        return dataOut;
    }

    // TODO: Make this method shorter, less than 100 lines
    public List<PigOutLogicalChunks> createLogicalChunks() throws FrontendException {
        Set<Operator> seenByDFS = new HashSet<Operator>();
        Map<LogicalPlan, PigOutLogicalChunks> mapping =
                new HashMap<>();

        while ( !visitQueue.isEmpty() ) {
            Operator s = visitQueue.poll();
            LogicalRelationalOperator lro = (LogicalRelationalOperator) s;
            
            log.debug("Visit: " + lro);
		
            // for each s, we may need to create a new "chunk"
			// each partition is created by DFS from a source
            if (!seenByDFS.add(s)) continue;

            // Create a new logical plan for the new chunk
            LogicalPlan newPlan = new LogicalPlan();
            newPlan.add(s);

            subPlans.put(s, newPlan);

            List<Operator> preds = this.plan.getPredecessors(s);
            if (preds != null)
                planPreds.put(newPlan, new LinkedList<>(preds));

            log.debug(lro.getAlias() + " is the root of new chunk");
            
            discoverPartition(s, seenByDFS, newPlan);

            String nameNode = this.siteAssignment.get(s);
            String hostname = nameNode.split(":")[0];
            String portNum = this.context.getProperties().getProperty(hostname + ".job.tracker").split(":")[1];
            int port = new Integer(portNum);

            PigOutCluster clusterInfo = getPigOutCluster(hostname, port);

            long dataIn = computeDataInSize(newPlan);
            long dataOut = computeDataOutSize(newPlan);

            addStores(newPlan);
            addLoads(newPlan);

            Iterator<Operator> it = newPlan.getOperators();
            while (it.hasNext()) {
                Operator op = it.next();
                op.setPlan(newPlan);
            }

            PigOutLogicalChunks chunk;
            chunk = new PigOutLogicalChunks(newPlan, context, clusterInfo, dataIn, dataOut);
            chunk.setAlias(lro.getAlias());
            mapping.put( newPlan, chunk );
        }

        for (LogicalPlan lp : mapping.keySet()) {
            List<Operator> succs = planSuccs.get(lp);
            List<Operator> preds = planPreds.get(lp);
            PigOutLogicalChunks chunk = mapping.get(lp);

            if (succs != null) {
                for (Operator succ : succs) {
                    LogicalPlan succLP = (LogicalPlan) succ.getPlan();
                    PigOutLogicalChunks succChunk = mapping.get(succLP);
                    succChunk.addPredecessors(chunk);
                    log.debug(succChunk.getAlias() + " depends on " + chunk.getAlias());
                }
            }

            if (preds != null) {
                for (Operator pred : preds) {
                    LogicalPlan predLP = (LogicalPlan) pred.getPlan();
                    PigOutLogicalChunks predChunk = mapping.get(predLP);
                    chunk.addPredecessors(predChunk);
                    log.debug(chunk.getAlias() + " depends on " + predChunk.getAlias());
                }
            }
        }

        List<PigOutLogicalChunks> list_chunks = new LinkedList<>();

        for (PigOutLogicalChunks chunk : mapping.values()) {
            list_chunks.add( chunk );
        }

        return list_chunks;
    }

    public PigOutCluster getPigOutCluster(String hostname, int portNum) {
        PigOutCluster clusterInfo = null;
        int numNodes = 1;

        try {
            Configuration conf = new Configuration();
            InetSocketAddress sockAddr =
                new InetSocketAddress(hostname, portNum);

            JobClient client = new JobClient( sockAddr, conf );

            ClusterStatus status = client.getClusterStatus(false);
            numNodes = status.getTaskTrackers();
            float defaultFactor = 1.0f;
            clusterInfo = new PigOutCluster(hostname, portNum, numNodes, defaultFactor, defaultFactor);

        } catch (IOException ex) {
            log.debug("Cannot connect to " + hostname + ":" + portNum);
            log.debug("Will set numNodes as " + numNodes);
        }

        return clusterInfo;
    }

    /*
     * This methods add LOLoad operator to a given (incomplete)
     * logical plan, so that the logical plan can be compiled to
     * MapReduce plan correctly.
     * @param   chunk   a given logical plan
     */
    public void addLoads(LogicalPlan chunk) throws FrontendException {
        List<Operator> sources = new LinkedList<>(chunk.getSources());
        /*
        List<String> srcAliases = new LinkedList<>();

        Iterator<Operator> opIter = chunk.getOperators();
        while (opIter.hasNext()) {
            Operator op = opIter.next();
            List<Operator> preds = chunk.getPredecessors(op);
            if (preds == null || preds.size() == 0) continue;
            for (Operator pred : preds) {
                // Is pred in chunk?
                LogicalRelationalOperator predLrop = (LogicalRelationalOperator) pred;
                if (null == chunk.findByAlias(predLrop.getAlias()))
                    srcAliases.add(predLrop.getAlias());

            }
        }
        */
        for (Operator src : sources) {
            if (src instanceof LOLoad) continue;
            addNewLOLoad(chunk, src);
        }
    }

    /*
     * This method adds LOStore operators to a given (incomplete)
     * logical plan, so that the logical plan can be compiled to
     * MapReduce plan.
     * @param   chunk   a given logical plan
     */
    public void addStores(LogicalPlan chunk) throws FrontendException {
        List<Operator> sinks = new LinkedList(chunk.getSinks());

        for (Operator sink : sinks) {
            if (sink instanceof LOStore) continue;
            log.info("LOStore is added to " + ((LogicalRelationalOperator)sink).getAlias());
            addNewLOStore(chunk, sink);
        }

    }

    /*
     *  debug: Prints out each partition's:
     *   -  root (starting node)
     *   -  its predecessors (operators, not plans), and
     *   -  its successors (again, operators)
     */
    private void printPartitioning() {
        for (Operator o : subPlans.keySet()) {
            LogicalPlan plan = subPlans.get(o);
            if (plan.getPredecessors(o) != null) continue;
            LogicalRelationalOperator lro = (LogicalRelationalOperator)o;
            log.debug("Root: " + lro.getAlias());
            log.debug(" Preds: " + planPreds.get(subPlans.get(o)));
            log.debug(" Succs: " + planSuccs.get(subPlans.get(o)));
        }
    }


	public Map<Operator, Collection<Operator>> getSlices() {
		return this.slices;
	}

	Map<Operator, Properties> sliceProps = new HashMap<Operator, Properties>();

	/**
	 * Returns HDFS namenode for a given jobtracker uri in host.name:port_number format.
	 * The mapping must be provided in conf/pigout.properties
	 * 
	 * @param jobtracker
	 * @return
	 * @throws FrontendException
	 */
	private String nameNodeOf(String jobtracker) throws FrontendException {
        String hostname;
		try {
            hostname = jobtracker.substring(0, jobtracker.indexOf(":"));
        } catch (NullPointerException npe) {
            throw new FrontendException("Format of JobTracker address is not correct: " + jobtracker);
        }
		return 
				this.context.getProperties().getProperty(hostname + ".namenode");
	}

	private static String getTimestamp(int offset) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, offset);
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		return formatter.format(cal.getTime());	
	}

	private List<Pair<Operator, Operator>> cuts = new LinkedList<Pair<Operator, Operator>>();

    private boolean isBinaryOp(LogicalRelationalOperator op) {
        if (op instanceof LOCogroup && op.getPlan().getPredecessors(op).size() > 1)
            return true;

        return (op instanceof  LOJoin || op instanceof LOUnion || op instanceof LOCross);
    }

	private boolean needToCut( Operator node, Operator succ ) {
		LogicalRelationalOperator lnode = (LogicalRelationalOperator) node;
		LogicalRelationalOperator lsucc = (LogicalRelationalOperator) succ;

        // If the pattern is LOAD followed by ForEach or Filter,
        // we must not cut.
        if (lnode instanceof LOLoad &&
                ( (lsucc instanceof LOForEach) || (lsucc instanceof LOFilter) ))
            return false;

		if (siteAssignment.get(lnode).compareTo( siteAssignment.get(lsucc) ) != 0) {
			if (lsucc instanceof LOStore) 
				return false; // to avoid LOAD-STORE job. Need to "copy" output later
			else 
				return true;
		} else if ( isBinaryOp(lsucc) ) {
			// cut if join's another input is not assigned to the same cluster;
			// unless we lose parallelism
            return true;
            /*
			List<Operator> preds = lsucc.getPlan().getPredecessors(lsucc);
            if (preds.size() > 1) {
                for (Operator pred : preds) {
                    List<Operator> ppreds = pred.getPlan().getPredecessors(pred);
                    for (Operator ppred : ppreds) {
                        String predSite = siteAssignment.get(ppred);
                        String nodeSite = siteAssignment.get(node);

                        if (predSite.compareTo(nodeSite) != 0)
                            return true;
                    }
                }
            }
            */
		}
		return false;
	}
	
	private void depthFirst(Operator node,
			Collection<Operator> successors,
			Set<Operator> seen,
			Collection<Operator> slice) {

		if (successors == null) {
			return;
		}

		for (Operator suc : successors) {
			if ( needToCut( node, suc ) ) {
				// start a new dfs tree...
				// we can just skip. Loop in init() will take care of this "suc"
				log.debug("A new cut: " + ((LogicalRelationalOperator)node).getAlias()
						+ "->"
						+ ((LogicalRelationalOperator)suc).getAlias() );
				
				cuts.add( new Pair<Operator, Operator>(node, suc) );
				continue;
			} else {			
				if (seen.add(suc)) {
					slice.add(suc);
					Collection<Operator> newSuccs = Utils.mergeCollection(plan.getSuccessors(suc), plan.getSoftLinkSuccessors(suc));
					depthFirst(suc, newSuccs, seen, slice);
				}
			}
		}
	}

	private String getJobTrackerOf(String namenode) {
		Properties properties = this.context.getProperties();
		String hostname = namenode.substring(0, namenode.indexOf(":"));
		return properties.getProperty(hostname + ".job.tracker", "localhost");
	}

	private String genSchema(LogicalSchema schema) {
		if (schema == null) return null;

		StringBuilder sb = new StringBuilder();
		List<LogicalFieldSchema> fields = schema.getFields();

		int n_fields = 1;

		for (LogicalFieldSchema lfs : fields) {

            if (lfs.alias != null)
                sb.append(lfs.alias);
            else
                sb.append("f" + n_fields);
   
            if (n_fields < fields.size())
				sb.append(", ");
			n_fields++;
		}

		return sb.toString();
	}

}
