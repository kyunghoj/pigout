package edu.buffalo.cse.pigout.partitioner;

import edu.buffalo.cse.pigout.tools.PigOutUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.LineSeparator;
import org.jdom2.output.XMLOutputter;

/*
 * Generator for Oozie Coordinator application from a PigOutLogicalChunk
 */
public class OozieCoordGen {
    private final Log log = LogFactory.getLog(getClass());

    PigOutLogicalChunks chunk;
    Element coordAppElem;

    Datasets dss = new Datasets();

    Namespace ns = Namespace.getNamespace("uri:oozie:coordinator:0.1");

    // Configuration info loaded from pigout.properties
    Properties conf;
    Properties p;
    String namenode;

    public OozieCoordGen(PigOutLogicalChunks chunk, Properties conf)
        throws FrontendException {

        this.chunk = chunk;
        this.conf = conf;
   
        // information to be written to coordinator.properties
        p = new Properties();

        PigOutCluster c = chunk.getPigOutCluster();
        
        namenode = "hdfs://" + conf.getProperty(c.getnodeName() + ".namenode");
        String hdfsPrefix = conf.getProperty("pigout.hdfs.dir.prefix") + "/" + "workflows/";
        String alias = chunk.getAlias();
        String jobTracker =  c.getnodeName() + ":" + c.getPortNum();
        String jobName = conf.getProperty("jobName");

        p.setProperty("oozie.use.system.libpath", "true");
        p.setProperty("nameNodeIn", namenode);
        p.setProperty("jobTracker", jobTracker);
        
        p.setProperty("coord_app_name", jobName + "_" + alias);
        p.setProperty("oozie.coord.application.path", 
                namenode + hdfsPrefix + "/" + jobName + "/" + alias);

        String workflow_app_path = 
                namenode + hdfsPrefix + "/" + jobName + "/" + alias;

        p.setProperty("workflow_name", jobName + "_" + alias);
        p.setProperty("workflow_app_path", workflow_app_path);
        p.setProperty("workflow_pig_script", alias + ".pig");

        p.setProperty("jobName", jobName);

        p.setProperty("start", PigOutUtils.getTimestamp(-5));
        p.setProperty("end", PigOutUtils.getTimestamp(600));
        p.setProperty("min_frequency", "45");
        //p.setProperty("frequency", "60");
        p.setProperty("flag_file", "_SUCCESS");
        p.setProperty("timezone", "America/New_York");
    }

    public Properties getCoordProperties() {
        return this.p;
    }

    public Element generate() throws IOException {
        coordAppElem = new Element("coordinator-app", ns);

        // these will be defined in coordinator.properties
        coordAppElem.setAttribute("name", "${coord_app_name}");
        coordAppElem.setAttribute("start", "${start}");
        coordAppElem.setAttribute("end", "${end}");
        coordAppElem.setAttribute("timezone", "${timezone}");
        coordAppElem.setAttribute("frequency", "${coord:days(365)}");
        
        LogicalPlan lp = this.chunk.getPlan();
        int noSrc  = lp.getSources().size();
        int noSink = lp.getSinks().size();

        // datasets and input-events
        addDatasetsElem();
        createDataIOEvents();

        // action
        addActionElem();

        return coordAppElem;
    }

    public void writeProperties(String path) {
        File localProperties = null;
        FileOutputStream fop = null;

        try {
            localProperties = new File( path );
            if (!localProperties.getParentFile().exists())
                localProperties.getParentFile().mkdirs();

            fop = new FileOutputStream(localProperties);
            this.p.store(fop, "comment");

        } catch (IOException e) {
            log.error("Write to local file failed.");
            e.printStackTrace();
            return;

        } finally {
            try {
                if (fop != null) fop.close();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

        }
    }

    public void writeXMLDoc( String path ) {

        File localXMLFile;
        FileOutputStream fop = null;
        Document doc = new Document(coordAppElem);

        try {
            localXMLFile = new File(path);
            if ( !localXMLFile.getParentFile().exists() )
                localXMLFile.getParentFile().mkdirs();

            fop = new FileOutputStream(localXMLFile);

            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            Format xoutFormat = xout.getFormat();
            xoutFormat.setLineSeparator(LineSeparator.NL);
            xout.output(doc, fop);
            //xout.output(doc, System.out);

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

    private void addActionElem() {
        CoordAction coordAction = new CoordAction();
        String chunkAlias = this.chunk.getAlias();
        String workflow_app_path = "PigOut_" + chunkAlias;
        Workflow wf = new Workflow(workflow_app_path, p);
        coordAction.add(wf);

        coordAppElem.addContent(coordAction.generate());
    }

    private void addDatasetsElem() {
        LogicalPlan lp = this.chunk.getPlan();
        List<Operator> sources = lp.getSources();
        List<Operator> sinks = lp.getSinks();
        
        int inCnt = 1, outCnt = 1;

        for (Operator source : sources) {
            // Create <dataset> for each source
            Dataset ds = new Dataset();
            LOLoad load = (LOLoad) source;

            ds.name = "input" + inCnt;
            ds.uri = load.getFileSpec().getFileName();
            if (!ds.uri.startsWith("hdfs")) {
                ds.uri = namenode + ds.uri;
            }

            ds.start = "${start}";
            ds.frequency = "${min_frequency}";
            ds.timezone = "${timezone}";

            dss.addDataset(ds);
            this.p.setProperty(ds.name, ds.uri);
            inCnt++;
        }

        for (Operator sink : sinks) {
            LOStore store = (LOStore) sink;

            Dataset ds = new Dataset();
            ds.name = "output" + outCnt;
            ds.uri = store.getFileSpec().getFileName();;

            ds.start = "${start}";
            ds.frequency = "${min_frequency}";
            ds.timezone = "${timezone}";

            dss.addDataset(ds);
            this.p.setProperty(ds.name, ds.uri);
            outCnt++;
        }

        coordAppElem.addContent(dss.generate());
    }

    private void createDataIOEvents() {
        LogicalPlan lp = this.chunk.getPlan();

        InputOutputEvts inEvts = new InputOutputEvts("input-events");
        InputOutputEvts outEvts = new InputOutputEvts("output-events");

        int inCnt = 1;
        int outCnt = 1;

        for (Operator source : lp.getSources()) {
            // Create <dataset> for each source
            LOLoad load = (LOLoad) source;
            String filename = load.getFileSpec().getFileName();
            String datasetName = "input" + inCnt;

            // Create DataInOutEvt
            String dataInName = "din" + inCnt;
            DataInOutEvt din_evt
                    = new DataInOutEvt(true, dataInName, datasetName, "${coord:current(0)}");

            inEvts.add(din_evt);

            inCnt++;
        }

        for (Operator sink : lp.getSinks()) {
            LOStore store = (LOStore) sink;

            String datasetName = "output" + outCnt;

            String dataOutName = "dout" + outCnt;
            DataInOutEvt dout_evt
                    = new DataInOutEvt(false, dataOutName, datasetName, "${coord:current(0)}");

            outEvts.add(dout_evt);

            outCnt++;
        }

        coordAppElem.addContent(inEvts.generate());
        coordAppElem.addContent(outEvts.generate());
    }

    class Dataset {
        protected String name;
        protected String frequency;
        protected String start;
        protected String timezone;
        protected String uri;

        protected Element datasetElem = null;

        public Element generate() {
            if (datasetElem != null) return datasetElem;

            datasetElem = new Element("dataset", ns);
            datasetElem.setAttribute("name", name);
            datasetElem.setAttribute("frequency", frequency);
            datasetElem.setAttribute("initial-instance", start);
            datasetElem.setAttribute("timezone", timezone);

            Element uriTemplate = new Element("uri-template", ns);
            uriTemplate.setText("${" + name + "}");

            Element done_flag = new Element("done-flag", ns);
            done_flag.setText("_SUCCESS");

            datasetElem.addContent(uriTemplate);
            datasetElem.addContent(done_flag);

            return datasetElem;
        }
    }

    class Datasets {
        List<Dataset> datasets;
        public Datasets() {
            datasets = new ArrayList<>();
        }

        public void addDataset(Dataset ds) {
            this.datasets.add(ds);
        }

        public Element generate() {
            if (this.datasets == null || this.datasets.size() == 0) return null;

            Element datasetsElem = new Element("datasets", ns);
            for (Dataset ds : datasets) {
                datasetsElem.addContent(ds.generate());
            }

            return datasetsElem;
        }
    }

    class InputOutputEvts {
        private List<DataInOutEvt> inputEvts = new ArrayList<>();
        private String elemName;

        public InputOutputEvts(String element) {
            this.elemName = element;
        }

        public void add(DataInOutEvt evt) {
            inputEvts.add(evt);
        }

        public Element generate() {
            Element elem = new Element(this.elemName, ns);
            for (DataInOutEvt evt : inputEvts) {
                elem.addContent(evt.generate());
            }
            return elem;
        }
    }

    class DataInOutEvt {
        String name;
        String dataSetName;
        String instance;

        boolean isInput = true;

        public DataInOutEvt(boolean isInput, String name, String dataset, String instance) {
            this.isInput = isInput;
            this.name = name;
            this.dataSetName = dataset;
            this.instance = instance;
        }

        public Element generate() {
            Element event;
            if (isInput)
                event = new Element("data-in", ns);
            else
                event = new Element("data-out", ns);

            event.setAttribute("name", this.name);
            event.setAttribute("dataset", this.dataSetName);

            Element instance = new Element("instance", ns);
            instance.setText("${coord:current(0)}");
            event.addContent(instance);

            return event;
        }
    }

    class CoordAction {
        List<Workflow> wfs;

        public CoordAction() {
            wfs = new ArrayList<>();
        }

        public void add(Workflow wf) {
            wfs.add(wf);
        }

        public List<Workflow> getWorkflows() {
            return this.wfs;
        }

        public Element generate() {
            Element action = new Element("action", ns);
            for (Workflow wf : wfs) {
                action.addContent(wf.generate());
            }

            return action;
        }
    }

    class Workflow {
        String app_path;
        Properties configurations;

        public Workflow(String app_path, Properties conf) {
            this.app_path = app_path;
            this.configurations = conf;
        }

        public Element generate() {
            Element wf = new Element("workflow", ns);
            Element app_path = new Element("app-path", ns);
            app_path.setText("${workflow_app_path}");
            wf.addContent(app_path);

            // Add configuration parameters
            Element confElem = new Element("configuration", ns);
            Set<String> conf_keys = configurations.stringPropertyNames();
            for (String name : conf_keys) {
                Element propElem = new Element("property", ns);
                Element propNameElem = new Element("name", ns);
                propNameElem.setText(name);
                propElem.addContent( propNameElem );

                Element propValElem = new Element("value", ns);
                propValElem.setText(configurations.getProperty(name));
                propElem.addContent( propValElem );

                confElem.addContent( propElem );
            }

            wf.addContent( confElem );

            return wf;
        }
    }
}
