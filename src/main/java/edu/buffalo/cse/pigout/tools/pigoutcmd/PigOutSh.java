package edu.buffalo.cse.pigout.tools.pigoutcmd;

import edu.buffalo.cse.pigout.PigOutServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.tools.pigstats.PigStatsUtil;

import java.io.BufferedReader;
import java.io.IOException;

public class PigOutSh
{
    private final Log log = LogFactory.getLog(getClass());
    
    BufferedReader  in;
    PigOutServer    pigOut;
    PigServer       pig;
    PigOutParser parser;

    public PigOutSh(BufferedReader in, PigContext cxt) throws ExecException
    {
        this.in = in;
        this.pigOut = new PigOutServer(cxt);
        
        if (in != null)
        {
            parser = new PigOutParser(this.in);
            parser.setParams(this.pigOut);
        }
    }
/*
    public void setConsoleReader(ConsoleReader c)
    {
        c.addCompletor(new PigCompletorAliases(pig));
        c.addCompletor(new PigCompletor());
        parser.setConsoleReader(c);
        return;
    }
*/
    public void run() {
        //boolean verbose = "true".equalsIgnoreCase(pigOut.getContext().getProperties().getProperty("verbose"));
        boolean verbose = true;

        while (true) {
            try {
                /*
                PigStatsUtil.getEmptyPigStats();
                */
                parser.setInteractive(true);
                parser.parseStopOnError();
                break;
            } catch (Throwable t) {
                //LogUtils.writeLog(t, pigOut.getContext().getProperties().getProperty("pigout.logfile"), log, verbose, "PigOut Stack Trace");
                parser.ReInit(in);
            }
        }
    }

    public int[] exec() throws Throwable {
        //boolean verbose = "true".equalsIgnoreCase(pigOut.getContext().getProperties().getProperty("verbose"));
        boolean verbose = true;

        try {
            PigStatsUtil.getEmptyPigStats();
            parser.setInteractive(false);
            return parser.parseStopOnError();
            /*
            parser.parseStopOnError();
            parser.partition();
            parser.generateOozieApps();
            return parser.submitOozieApps();
            */
        } catch (Throwable t) {
            //LogUtils.writeLog(t, pigOut.getContext().getProperties().getProperty("pigout.logfile"), log, verbose, "PigOut Stack Trace");
            throw (t);
        }
    }
    
    public void parseAndBuild() throws Throwable {
        try {
            PigStatsUtil.getEmptyPigStats();
            parser.setInteractive(false);
            parser.parseAndBuild( true );
        } catch (Throwable t) {
        	t.printStackTrace();
            throw (t);
        }
    }
    
    public void partition() throws FrontendException, IOException
    {
    	this.pigOut.partitionBatch();
    }

    public void checkScript(String scriptFile) 
    {
        //log.info("checkScript(String scriptFile) is not implemented.");
    	boolean verbose = "true".equalsIgnoreCase(pigOut.getContext().getProperties().getProperty("verbose"));
    	try {
    		
    	} catch (Throwable t) {
    		
    	}
    	
        return;
    }
}
