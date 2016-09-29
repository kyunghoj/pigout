package edu.buffalo.cse.pigout.partitioner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.LineSeparator;
import org.jdom2.output.XMLOutputter;


/*
 * Generate Oozie workflow and coordinator job instance.
 * WorkflowGenerator is instantiated for each sub-query (script, plan, whatever)
 */

public class WorkflowGenerator {
	private final Log log = LogFactory.getLog(getClass());
	
    protected String jobTracker;
    protected String nameNodeIn;
    protected String nameNodeOut;
    
    protected String nameNode;

    private Map<String, String> dataIns; // dataset paramter(e.g., input1), actual path
    private Map<String, String> dataOuts;
    private LogicalRelationalOperator rootOp;
    
    protected Configuration conf;

    private PigContext context;
    
    private String pigscript = null; // file name only
    private Properties props = null;

    private Document workflowXMLDoc = null;
    private Document coordXMLDoc = null;

    private void loadXMLTemplates() throws JDOMException, IOException {
        // 1. Load oozie-templates/workflow.xml and coordinator.xml
        // 2. Update or add nodes
        // 3. Write workflow.xml and coordinator.xml to oozie/
        File wfTemplate; 
        File coordTemplate;
            
        String dir = context.getProperties().getProperty("pigout.oozie.templates", "./oozie-templates");
        wfTemplate = new File(dir + "/workflow.xml");
        coordTemplate = new File(dir + "/coordinator.xml");

        this.workflowXMLDoc = new SAXBuilder().build( wfTemplate );
        this.coordXMLDoc = new SAXBuilder().build( coordTemplate );
    }

    public WorkflowGenerator(
    		PigContext context,
    		Properties props,
    		Operator root,
    		Map<String, String> dataIns,
    		Map<String, String> dataOuts) {
    	
        this.context = context;
        this.props = props;
        this.rootOp = (LogicalRelationalOperator)root;
        this.dataIns = dataIns;
        this.dataOuts = dataOuts;
        
        try {
        	this.loadXMLTemplates();
        } catch (org.jdom2.JDOMException jdom_e) {
            log.error("Cannot parse Oozie XML templates in " 
                    + this.context.getProperties().getProperty(
                    		"pigout.oozie.templates",
                    		"./oozie-templates"));
            jdom_e.printStackTrace();
            return;
        } catch (IOException ioe) {
        	log.error("Cannot read Oozie XML templates in "
        			+ this.context.getProperties().getProperty(
        					"pigout.oozie.templates", 
        					"./oozie-templates"));
        	ioe.printStackTrace();
        	return;
        }
    }


    private boolean writePropertiesFile(Properties props, String dir) {
        File localPropsFile = null;
        FileOutputStream fop = null;

        try {
            localPropsFile = new File( dir, "coordinator.properties" );
            
            if ( !localPropsFile.getParentFile().exists() )
                localPropsFile.getParentFile().mkdirs();

            fop = new FileOutputStream(localPropsFile);
            props.store(fop, "comment");
        } catch (IOException e) {
            log.error("Write to local file failed.");
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fop != null) fop.close();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    private boolean writeOozieXML(Document doc, String dir, String filename) {
        // 1. Write XML file to local filesystem
        File localXMLFile = null;
        FileOutputStream fop = null;
        try {

            localXMLFile = new File( dir + "/" , filename );

            if ( !localXMLFile.getParentFile().exists() ) 
                localXMLFile.getParentFile().mkdirs();
            
            fop = new FileOutputStream(localXMLFile);

            
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            Format xoutFormat = xout.getFormat();
            xoutFormat.setLineSeparator(LineSeparator.NL);
            xout.output(doc, fop);

            fop.close();
                       
        } catch (IOException e) {
            log.error("Write to local file failed.");
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fop != null) fop.close();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }
    
    // Use this method if hadoop dfs -put works.
    private void copyToHDFS(String srcPath, String nameNode, String destPath) {
    	Configuration fsConf = new Configuration();
    	
    	fsConf.set( "fs.default.name", nameNode);
    	try {
    		org.apache.hadoop.fs.FileSystem fs = 
    				org.apache.hadoop.fs.FileSystem.get(fsConf);
    		org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(srcPath);
    		org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(destPath);
    		log.debug("copyToHDFS: src: " + srcPath + " dst: " + destPath);
    		fs.copyFromLocalFile(src, dst);
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    }
    
    private void submitCoordToOozie(Path dir) {
    	Path propPath = dir.resolve("coordinator.properties");
    	// 1. "workflow_app_path"
    	// 2. "oozie.coord.application.path"
    	
    	// "where to copy" is per "alias" basis.
    	// Retrieve nameNodeIn from coordinator.properties
    	// Copy from local to remote staging directory
    	Properties p = new Properties();
    	
    	try ( FileReader inStream = new FileReader(propPath.toAbsolutePath().toString()) ) {
    		p.load(inStream);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}

    	String coordAppPath = p.getProperty("oozie.coord.application.path");
    	String nameNodeIn = p.getProperty("nameNodeIn");
    	
    	this.copyToHDFS(dir.toString(), nameNodeIn, coordAppPath);
    	
    }
    
    public boolean submitToOozie() {
    	// 1. Information on Oozie server (URL, ...)
    	// 2. Figure out what "Job" directory this should submit
    	// 3. For each sub-directory (each has a coordinator and workflow app)
    	// 4. set up a staging (temp) directory on each HDFS namenode
    	Properties p = this.context.getProperties();
    	
    	String oozieFileDir = p.getProperty("pigout.oozie.local");
    	String jobName = p.getProperty("jobName"); // will be directory name
    	
    	// Build a list of local directories
    	Path localAppPath = Paths.get(oozieFileDir, jobName);
    	List<Path> coorDirs = new ArrayList<Path>();
    	
    	try (DirectoryStream<Path> stream = Files.newDirectoryStream(localAppPath)) {
    	    for (Path file: stream) {
    	    	if (Files.isDirectory(file)) {
    	    		coorDirs.add(file);
    	    		log.debug("A new coordinator dir: " + file);

    	    		this.submitCoordToOozie(file);
    	    	}
    	    }
    	} catch (IOException | DirectoryIteratorException x) {
    		log.error("Error occurred while building a list of Oozie workflow definitions");
    	    System.err.println(x);
    	}
    	
    	return true;
    }

    public void buildOozieWorkflowApp() throws IOException {
        Namespace ns = Namespace.getNamespace("uri:oozie:workflow:0.2");

        // 1. Start with the root, <workflow-app name="..."
        Element wf = this.workflowXMLDoc.getRootElement();
        Element action = wf.getChild("action", ns);
        Element pig = action.getChild("pig", ns);

        // 2. <preapre>
        Element prepare = pig.getChild("prepare", ns);
        if (prepare == null) {
            prepare = new Element("prepare", ns);
            pig.addContent(prepare);
        }

        for ( String output : this.dataOuts.keySet() ) {
            Element delete = new Element("delete", ns);
            delete.setAttribute( "path", String.format("${%s}", output) );
            prepare.addContent(delete);
        }

        // 3. parameters for pig script
        for ( String p : this.dataIns.keySet() ) {
            Element param = new Element("param", ns);
            param.setText( String.format("%s=${%s}", p, p) );
            pig.addContent( param );
        }
        
        for ( String p : this.dataOuts.keySet() ) {
            Element param = new Element("param", ns);
            param.setText( String.format("%s=${%s}", p, p) );
            pig.addContent( param );
        }
        
        // 4. ssh-copy (about copying datasets to another cluster)
        Element ok = action.getChild("ok", ns);
        if (ok == null) {
        	ok = new Element("ok", ns);
        	ok.setAttribute("to", props.getProperty("okTo"));
        	action.addContent(ok);
        } else {
        	ok.setAttribute("to", props.getProperty("okTo"));
        }
        
        // 5. all set! Write to local filesystem
        String wfDir = 
        		this.context.getProperties().getProperty("pigout.oozie.local") + 
        		"/" +
        		this.context.getProperties().getProperty("jobName") + 
        		"/" +
                this.rootOp.getAlias();
        
        this.writeOozieXML( this.workflowXMLDoc, 
        		wfDir,
                "workflow.xml");

        this.writePropertiesFile( this.props, wfDir);
    }
    
    public void buildOozieCoordApp() throws IOException {
    	// 0. Prelim
    	Properties pigoutProps = this.context.getProperties();
    	String oozieDir = pigoutProps.getProperty("pigout.oozie.local");
    	
        // 1. Start with the root, <coordinator-app name="..." 
        Element coord = this.coordXMLDoc.getRootElement();
        Namespace ns = Namespace.getNamespace("uri:oozie:coordinator:0.1");

        // 2. define datasets
        //List<Element> datasetsElems = coord.getChildren();
        Element datasetsElem = coord.getChild("datasets", ns);
        
        // 3. specify input/output events, such as datasets
        Element inputevents = coord.getChild("input-events", ns);
        Element outputEvts = coord.getChild("output-events", ns);
     
        // TODO: Redundant code for handling inputs and outputs
        // Handling outputs
        if (this.dataOuts != null) {
        	int outCount = 1;
        	for (String dataset_param : this.dataOuts.keySet() ) {
        		// dataset
        		Element dataset = new Element("dataset", ns);
            	dataset.setAttribute("name", "output" + outCount);
        		dataset.setAttribute("frequency", "${min_frequency}");
        		dataset.setAttribute("initial-instance", "${start}");
        		dataset.setAttribute("timezone", "${timezone}");

        		Element uri = new Element("uri-template", ns);
        		String baseUri = "${" + dataset_param + "}/";
        		// if it's a synchronous job
        		//baseUri = baseUri + "${YEAR}/${MONTH}/${DAY}/"; // if it's a daily job
        		uri.setText( baseUri );
        		dataset.addContent(uri);

        		Element doneFlag = new Element("done-flag", ns);
        		doneFlag.setText("${flag_file}");
        		dataset.addContent(doneFlag);

        		Element data_out;
        		data_out = new Element("data-out", ns);
        		data_out.setAttribute("name", "dout"+(outCount));
        		data_out.setAttribute("dataset", "output" + outCount);
        		datasetsElem.addContent(dataset);
        		
        		Element inst = new Element("instance", ns);
        		inst.setText("${coord:current(0)}");
        		data_out.addContent(inst);

        		outputEvts.addContent(data_out);

        		// 4. define action (workflow)
        		Element action = coord.getChild("action", ns);
        		if (action == null) 
        			action = new Element("action", ns);
        		
        		Element workflow = action.getChild("workflow", ns);
        		if (workflow == null)
        			workflow = new Element("workflow", ns);

        		Element conf = workflow.getChild("configuration", ns);
        		Element property = new Element("property", ns);
        		Element name = new Element("name", ns);
        		name.setText(dataset_param);

        		Element value = new Element("value", ns);
        		value.setText("${coord:dataOut(\'dout" + outCount + "\')}");
        		property.addContent(name);
        		property.addContent(value);
        		conf.addContent(property);

        		property = new Element("property", ns);
        		name = new Element("name", ns);
        		name.setText(dataset_param +"To");
        		value = new Element("value", ns);
        		value.setText( props.getProperty(dataset_param) );
        		property.addContent(name);
        		property.addContent(value);
        		conf.addContent(property);
        		
        		outCount++;
        		
        	}
        }
        
        // Handling inputs
        if (this.dataIns != null) {
        	int inCount = 1;
        	for (String dataset_param : this.dataIns.keySet()) {
        		// define a dataset
        		Element dataset; 

        		dataset = new Element("dataset", ns);

        		dataset.setAttribute("name", "input" + inCount);
        		dataset.setAttribute("frequency", "${min_frequency}");
        		dataset.setAttribute("initial-instance", "${start}");
        		dataset.setAttribute("timezone", "${timezone}");

        		Element uri = new Element("uri-template", ns);
        		String baseUri = "${" + dataset_param + "}/";
        		// if it's a synchronous job
        		//baseUri = baseUri + "${YEAR}/${MONTH}/${DAY}/"; // if it's a daily job
        		uri.setText( baseUri );
        		dataset.addContent(uri);

        		Element doneFlag = new Element("done-flag", ns);
        		doneFlag.setText("${flag_file}");
        		dataset.addContent(doneFlag);
        		datasetsElem.addContent(dataset);

        		Element data_in;
        		data_in = new Element("data-in", ns);
        		data_in.setAttribute("name", "din"+(inCount));
        		data_in.setAttribute("dataset", "input" + inCount);
        		
        		
        		Element inst = new Element("instance", ns);
        		inst.setText("${coord:current(0)}");
        		data_in.addContent(inst);

        		inputevents.addContent(data_in);
        		

        		// 4. define action (workflow)
        		Element action = coord.getChild("action", ns);
        		if (action == null) 
        			action = new Element("action", ns);
        		
        		Element workflow = action.getChild("workflow", ns);
        		if (workflow == null)
        			workflow = new Element("workflow", ns);

        		Element conf = workflow.getChild("configuration", ns);
        		Element property = new Element("property", ns);
        		Element name = new Element("name", ns);
        		name.setText(dataset_param);

        		Element value = new Element("value", ns);
        		value.setText("${coord:dataIn(\'din" + inCount + "\')}");
        		property.addContent(name);
        		property.addContent(value);
        		conf.addContent(property);

        		inCount++;
        	}
            // let's assume one STORE now...
        }
        
        // 5. Write XML file to local filesystem

        String wfDir =
        		pigoutProps.getProperty("pigout.oozie.local") + "/" +
        		pigoutProps.getProperty("jobName") +     		"/" + 
        		this.rootOp.getAlias();
        
        this.writeOozieXML( this.coordXMLDoc,
        		wfDir,
        		"coordinator.xml");
        
    }
}
