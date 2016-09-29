package edu.buffalo.cse.pigout.partitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.LineSeparator;
import org.jdom2.output.XMLOutputter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.List;
import java.util.Properties;

/*
 * Code for generating worflow.xml
 * April 7, 2014
 * kyunghoj
 */

public class OozieWorkflowGen {
    private final Log log = LogFactory.getLog(getClass());

    Properties conf;        // System-wide properties
    Properties execConf;    // Configuration for Execution engine (Hadoop, ...)
    Element workflowAppElem;// Root element of XML

    boolean isCopyOnlyWorkflow = true;

    PigOutLogicalChunks chunk;
    List<PigOutLogicalChunks> succChunks;
    Namespace ns = Namespace.getNamespace("uri:oozie:workflow:0.2");

    public OozieWorkflowGen( PigOutLogicalChunks chunk, List<PigOutLogicalChunks> list, Properties conf ) {
        this.chunk = chunk;
        this.conf  = conf;
        this.succChunks = list;

        this.execConf = new Properties();

        LogicalPlan clp = chunk.getLogicalPlan();
        List<Operator> sinks = clp.getSinks();
        for (Operator sink : sinks) {
            List<Operator> preds = clp.getPredecessors(sink);
            for (Operator pred : preds) {
                if (!(pred instanceof LOLoad)) {
                    isCopyOnlyWorkflow = false;
                }
            }
        }
    }

    // TODO: this is almost same as the method in OozieCoordGen
    public void writeXMLDoc( String path ) {
        File localXMLFile;
        FileOutputStream fop = null;

        Document doc = new Document(workflowAppElem);

        try {
            localXMLFile = new File(path);
            if ( !localXMLFile.getParentFile().exists() )
                localXMLFile.getParentFile().mkdirs();

            fop = new FileOutputStream(localXMLFile);


            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.getFormat().setLineSeparator(LineSeparator.NL);
            xout.output(doc, fop);
            //xout.output(doc, System.out); // debug

        } catch (IOException e) {
            log.error("Write to local file failed.");
            e.printStackTrace();
        } finally {
            try {
                if (fop != null) fop.close();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    // <pig>
    //   <job-tracker>${jobTracker}</job-tracker>
    //   <name-node>${nameNodeIn}</name-node>
    //   <prepare>
    //     <delete path="${output1}"/>
    //   <prepare>
    //   <script>${workflow_pig_script}</script>
    //   <configuration>
    //     <property>
    //       <name></name>
    //       <value></value>
    //     </property>
    //   </configuration>
    // </pig>
    private Element generatePigAction() throws IOException {
        Element pig = new Element("pig", ns);
        Element jobtracker = new Element("job-tracker", ns);
        jobtracker.setText("${jobTracker}");
        pig.addContent(jobtracker);

        Element namenode = new Element("name-node", ns);
        namenode.setText("${nameNodeIn}");
        pig.addContent(namenode);

        // Remove outputs from previous runs
        // temporarily do not add prepare elem, due to
        // Oozie's bug that fails a workflow if the directory specified
        // doesn't exist. kyunghoj, 4/24/2014
        /*
        Element prepare = new Element("prepare", ns);
        int sinkCount = 0;
        for ( Operator sink : this.chunk.getPlan().getSinks() ) {
            sinkCount++;
            Element deleteItem = new Element("delete", ns);
            deleteItem.setAttribute("path", "${output" + sinkCount + "}");
            prepare.addContent(deleteItem);
        }
        pig.addContent(prepare);
        */
        Element script = new Element("script", ns);
        script.setText("${workflow_pig_script}");
        pig.addContent(script);

        // Configuration for Exec Engine (Hadoop)
        Set<String> pNames = this.execConf.stringPropertyNames();
        if (pNames.size() != 0) {
            Element configuration = new Element("configuration", ns);
            for (String confKey : this.execConf.stringPropertyNames()) {
                String confVal = this.execConf.getProperty( confKey );
                Element property = new Element("property", ns);
                Element name = new Element("name", ns);
                name.setText(confKey);
                Element value = new Element("value", ns);
                value.setText(confVal);
                property.addContent(name);
                property.addContent(value);
                configuration.addContent(property);
            }
            pig.addContent(configuration);
        }

        return pig;

    }

    /*
     * Find where to go after a Pig action.
     * @returns     "end", "ssh_copy", or "fork_copy", 
     */
    private String findOkTo() {
        String okTo = null;

        if (this.succChunks == null) {
            return "end";
        }
        
        if (this.succChunks.size() == 0) {
            return "end";
        }

        int sinkCount = this.chunk.getPlan().getSinks().size();
        if (sinkCount == 1) {
            PigOutCluster cluster = chunk.getPigOutCluster();
            String hostname = cluster.getnodeName();
            String successorHost = this.succChunks.get(0).getPigOutCluster().getnodeName();
            if (hostname.compareTo(successorHost) == 0) {
                return "end";
            } else {
                return "ssh_copy";
            }
        } else {
            int remoteCopiesRequired = 0;
            for (PigOutLogicalChunks succ : succChunks) {
                String hostname = chunk.getPigOutCluster().getnodeName();
                String succHost = succ.getPigOutCluster().getnodeName();
                if (hostname.compareTo(succHost) != 0) {
                    remoteCopiesRequired++;
                }
            }
            if (remoteCopiesRequired > 1) {
                return "fork_copy";
            } else if (remoteCopiesRequired == 1) {
                return "ssh_copy";
            } else {
                return "end";
            }
        }
    }

    // fork and join, if we have multiple *intermediate* results
    // if they are just multiple *final* outputs, we don't need
    // ssh_copy.
    // (this might be another tuning knob, since we are not sure
    // if running multiple remote-copy helps or not.)
    private Element genForkElem() {

        int sinkCount = this.chunk.getPlan().getSinks().size();

        if (sinkCount >= 2) {
            // add fork
            // <fork name="fork-copy">
            //     <path start="ssh_copy_1"/>
            //     <path start="ssh_copy_2"/>
            //     ...
            // </fork>
        
            Element forkElem = new Element("fork", ns);
            for (int i = 1; i <= sinkCount; i++) {
                Element pathElem = new Element("path", ns);
                pathElem.setAttribute("start", "ssh_copy_" + i);
                forkElem.addContent(pathElem);
            }

            return forkElem;
        } else {
            return null;
        }
    }

    public Element generate() throws IOException {
        Element wfElem = new Element("workflow-app", ns);
        workflowAppElem = wfElem;
        String wfName = conf.getProperty(PigContext.JOB_NAME) + "_" + this.chunk.getAlias();
        workflowAppElem.setAttribute("name", wfName);

        if (isCopyOnlyWorkflow) {
            generateCopyOnlyWorkflow(wfElem);
        } else {
            generatePigWorkflow(wfElem);
        }
        return wfElem;
    }

    // TODO: implement this. 9/15/2014
    private void generateCopyOnlyWorkflow(Element wfElem) throws IOException {
        Element start = new Element("start", ns);
        start.setAttribute("to", "ssh_action");
        wfElem.addContent(start);

        Element start_action = new Element("action", ns);
        start_action.setAttribute("name", "ssh_copy");

        Operator sink = this.chunk.getPlan().getSinks().get(0);
        Element copyActionElem =
                genCopyAction("ssh_copy", sink, "end");
        wfElem.addContent(copyActionElem);

        // kill and end
        Element killElem = new Element("kill", ns);
        killElem.setAttribute("name", "fail");
        Element msgElem = new Element("message", ns);
        msgElem.setText("Workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        killElem.addContent( msgElem );
        wfElem.addContent( killElem );

        Element endElem = new Element("end", ns);
        endElem.setAttribute("name", "end");
        wfElem.addContent( endElem );
    }

    private Element generatePigWorkflow(Element wfElem) throws IOException {

        Element start = new Element("start", ns);
        start.setAttribute("to", "start_node");
        wfElem.addContent(start);

        Element start_action = new Element("action", ns);
        start_action.setAttribute("name", "start_node");

        // Add a Pig action
        Element pig = generatePigAction();
        start_action.addContent(pig);

        // Then, where to go? copy or finish?
        String okToDest = findOkTo();
        Element okToElem = new Element("ok", ns);
        okToElem.setAttribute("to", okToDest);

        Element errTo = new Element("error", ns);
        errTo.setAttribute("to", "fail");

        start_action.addContent(okToElem);
        start_action.addContent(errTo);
      
        // Finish generating start action
        wfElem.addContent(start_action);

        // Data copy action
        // 1. Single output:    go to ssh_copy
        // 2. Multiple outputs: go to fork
        if (okToDest.compareTo("ssh_copy") == 0) {
            Operator sink = this.chunk.getPlan().getSinks().get(0);
            Element copyActionElem = 
                genCopyAction("ssh_copy", sink, "end");

            wfElem.addContent(copyActionElem);
        
        } else if (okToDest.compareTo("fork_copy") == 0) {
            Element forkElem = genForkElem( );
            wfElem.addContent( forkElem );

            // add a copy action for each sink
            int i = 0;
            for (Operator sinkOp : this.chunk.getPlan().getSinks()) {
                i++;
                Element copyAction = genCopyAction("ssh_copy_" + i, 
                        sinkOp, "join_copy");
                wfElem.addContent(copyAction);
            }
        
            // each copy action goes to joincopy if succeeds
            // add join:
            // <join name="join_copy" to="end"/>
            Element joinElem = new Element("join", ns);
            joinElem.setAttribute("name", "join_copy");
            joinElem.setAttribute("to", "end");

            wfElem.addContent(joinElem);
        }

        // kill and end
        Element killElem = new Element("kill", ns);
        killElem.setAttribute("name", "fail");
        Element msgElem = new Element("message", ns);
        msgElem.setText("Workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        killElem.addContent( msgElem );
        wfElem.addContent( killElem );

        Element endElem = new Element("end", ns);
        endElem.setAttribute("name", "end");
        wfElem.addContent( endElem );

        return wfElem;
    }

    /*
     * Generate a copy action. 
     */
    private Element genCopyAction(String action_name, Operator sink, String okTo) {
        // shell action for copying intermediate result
        Element copy_action = new Element("action", ns);
        copy_action.setAttribute("name", action_name);

        Namespace shellNS = Namespace.getNamespace("uri:oozie:shell-action:0.1");

        Element shell = new Element("shell", shellNS);
        
        Element jobtracker = new Element("job-tracker", shellNS);
        jobtracker.setText("${jobTracker}");
        shell.addContent(jobtracker);

        Element namenode = new Element("name-node", shellNS);
        namenode.setText("${nameNodeIn}");
        shell.addContent(namenode);

        // Configuration for Hadoop
        Set<String> pNames = this.execConf.stringPropertyNames();
        if (pNames.size() != 0) {
            Element configuration = new Element("configuration", shellNS);
            for (String confKey : this.execConf.stringPropertyNames()) {
                String confVal = this.execConf.getProperty( confKey );
                Element property = new Element("property", shellNS);
                Element name = new Element("name", shellNS);
                name.setText(confKey);
                Element value = new Element("value", shellNS);
                value.setText(confVal);
                property.addContent(name);
                property.addContent(value);
                configuration.addContent(property);
            }
            shell.addContent(configuration);
        }
        // Command
        Element exec = new Element("exec", shellNS);
        exec.setText("remote-copy");
        shell.addContent(exec);
        
        // Command-line arguments
        // ssh_cp_src_dir
        String srcPath;
        String dstPath;

        if (isCopyOnlyWorkflow) {
            Operator src;
            src = sink.getPlan().getSources().get(0);
            srcPath = ((LOLoad)src).getFileSpec().getFileName();
            dstPath = ((LOStore)sink).getFileSpec().getFileName();
        } else {
            srcPath = dstPath = ((LOStore)sink).getFileSpec().getFileName();
        }
        shell.addContent(new Element("argument", shellNS).setText(srcPath));

        // ssh_cp_hostname (destination)
        // hardcoded...
        String dest_host = "hawaii";
        String dest_user = "hduser";
        StringBuffer destStrBuf = new StringBuffer();
        destStrBuf.append(dest_host);
        destStrBuf.append("@");
        destStrBuf.append(dest_user);

        //String dest_host = "ec2-54-202-159-53.us-west-2.compute.amazonaws.com";
        //String dest_user = "ubuntu";
        shell.addContent(new Element("argument", shellNS).setText(destStrBuf.toString()));

        // ssh_cp_dst_dir
        shell.addContent(new Element("argument", shellNS).setText(dstPath));

        // hadoop command path
        String hadoop_cmd_path = conf.getProperty(
			"hadoop.cmd.path",
			"/usr/local/hadoop/bin/hadoop");

        shell.addContent(new Element("argument", shellNS).setText(hadoop_cmd_path));

        // <file> </file>
        shell.addContent(new Element("file", shellNS).setText("remote-copy#remote-copy"));
        // Flow control
        Element okToElem = new Element("ok", ns);
        okToElem.setAttribute("to", okTo);
        
        Element errToElem = new Element("error", ns);
        errToElem.setAttribute("to", "fail");

        copy_action.addContent(shell);
        copy_action.addContent(okToElem);
        copy_action.addContent(errToElem);

        return copy_action;
    }
}

