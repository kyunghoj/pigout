package edu.buffalo.cse.pigout.tools.pigoutcmd;

import edu.buffalo.cse.pigout.PigOutServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.tools.ToolsPigServer;
import org.apache.pig.tools.pigscript.parser.ParseException;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class PigOutParser extends org.apache.pig.tools.pigscript.parser.PigScriptParser
{
    private static final Log log = LogFactory.getLog( PigOutParser.class );

    private PigOutServer mPigOut;
    private PigContext context;
    
    private ToolsPigServer pig;

    private Properties mConf;

    private boolean mDone;
    private boolean mLoadOnly;

    private boolean mInteractive = false;

    private ExplainState mExplain;

    private int mNumFailedJobs;
    private int mNumSucceededJobs;

    private boolean mScriptIllustrate;
    private boolean skipParseInRegisterForBatch = false;

    /**
     * Set whether to skip parsing while registering the query in batch mode
     * @param skipParseInRegisterForBatch
     */
    public void setSkipParseInRegisterForBatch(boolean skipParseInRegisterForBatch) {
        this.skipParseInRegisterForBatch  = skipParseInRegisterForBatch;
    }

    public PigOutParser(Reader in)
    {
        super(in);
        try {
            pig = new ToolsPigServer("local");
        } catch (Exception ex) {
            log.error("Unexpected exception.");
            ex.printStackTrace();
        }
    }

    public void setParams(PigOutServer pigout)
    {
        this.mPigOut = pigout;
        this.context = pigout.getContext();
    }

    public void parseAndBuild(boolean print) throws ParseException, IOException
    {
        this.parseStopOnError();
        this.mPigOut.parseAndBuild();
    }

    public int[] parseStopOnError(boolean sameBatch) throws ParseException, IOException
    {
        if (mPigOut == null) {
            throw new IllegalStateException();
        }

        if (!mInteractive && !sameBatch) {
            mPigOut.setBatchOn();
            mPigOut.setSkipParseInRegisterForBatch(true);
        }

        mDone = false;
        while (!mDone) {
            parse();
        }

        int [] res = {0, 0};
        return res;
    }

    public int[] parseStopOnError() throws ParseException, IOException
    {
        return parseStopOnError(false);
    }

    public void parseOnly() throws IOException, ParseException {
        mDone = false;
        this.mInteractive = false;
        this.mPigOut.setBatchOn();
        this.mPigOut.setSkipParseInRegisterForBatch(true);

        while (!mDone) {
            parse();
        }
    }

    public void ReInit(BufferedReader in)
    {
    }

    private static class ExplainState {
        public long mTime;
        public int mCount;
        public String mAlias;
        public String mTarget;
        public String mScript;
        public boolean mVerbose;
        public String mFormat;
        public boolean mLast;

        public ExplainState(String alias, String target, String script,
                boolean verbose, String format) {
            mTime = new Date().getTime();
            mCount = 0;
            mAlias = alias;
            mTarget = target;
            mScript = script;
            mVerbose = verbose;
            mFormat = format;
            mLast = false;
        }
    }

    public void prompt() {
    }

    public void setInteractive(boolean interactive) {
        mInteractive = interactive;
    }

    @Override
    protected void processPig(String cmd) throws IOException
    {
        int start = 1;
        if (!mInteractive) {
            start = getLineNumber();
        }

        if (cmd.charAt(cmd.length() - 1) != ';') {
            mPigOut.registerQuery(cmd + ";", start);
        }
        else {
            mPigOut.registerQuery(cmd, start);
        }
    }

    @Override
    protected void printAliases() throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void printClear() {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void printHelp() {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processCD(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processCat(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processCopy(String arg0, String arg1) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processCopyFromLocal(String arg0, String arg1)
    throws IOException {
    // TODO Auto-generated method stub
    log.info("Not implemented.");
    }

    @Override
    protected void processCopyToLocal(String arg0, String arg1)
    throws IOException {
    // TODO Auto-generated method stub
    log.info("Not implemented.");
    }

    @Override
    protected void processDescribe(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processDump(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processExplain(String alias, String script, boolean isVerbose,
            String format, String target, 
            List<String> params, List<String> files)
    throws IOException, ParseException {
    if (mPigOut.isBatchOn()) {
        mPigOut.parseAndBuild();
    }

    if ("@".equals(alias)) {
        alias = mPigOut.getLastRel();
    }


    }

    protected void processExplain(String alias, String script, boolean isVerbose,
            String format, String target,
            List<String> params, List<String> files,
            boolean dontPrintOutput)
        throws IOException, ParseException {

        if (null != mExplain) {
            return;
        }

        try {
            mExplain = new ExplainState(alias, target, script, isVerbose, format);

            if (script != null) {
                setBatchOn();
                try {
                    loadScript(script, true, true, false, params, files);
                } catch(IOException e) {
                    discardBatch();
                    throw e;
                } catch (ParseException e) {
                    discardBatch();
                    throw e;
                }
            }

            mExplain.mLast = true;
            explainCurrentBatch(dontPrintOutput);

        } finally {
            if (script != null) {
                discardBatch();
            }
            mExplain = null;
        }
    }

    private void explainCurrentBatch(boolean dontPrintOutput) {
        // TODO Auto-generated method stub

    }

    private void discardBatch() throws IOException {
        if (mPigOut.isBatchOn())
            mPigOut.discardBatch();
    }
    private String runPreprocessor(String scriptPath, List<String> params, List<String> paramFiles)
        throws IOException, ParseException {

        PigContext context = mPigOut.getContext();
        BufferedReader reader = new BufferedReader(new FileReader(scriptPath));
        return context.doParamSubstitution(reader, params, paramFiles);
    }

    private void loadScript(String script, boolean batch, boolean loadOnly, boolean illustrate,
            List<String> params, List<String> files)
        throws IOException, ParseException {

        Reader inputReader;
        boolean interactive;

        mPigOut.getContext().setParams(params);
        mPigOut.getContext().setParamFiles(files);

        try {
            FetchFileRet fetchFile = FileLocalizer.fetchFile(mConf, script);
            String cmds = runPreprocessor(fetchFile.file.getAbsolutePath(), params, files);

            inputReader = new StringReader(cmds);
            interactive = false;
        } catch (FileNotFoundException fnfe) {
            throw new ParseException("File not found: " + script);
        } catch (SecurityException se) {
            throw new ParseException("Cannot access file: " + script);
        }

        PigOutParser parser = new PigOutParser(inputReader);
        parser.setParams(mPigOut);
        //parser.setConsoleReader(reader);
        parser.setInteractive(interactive);
        parser.setLoadOnly(loadOnly);
        /*
           if (illustrate)
           parser.setScriptIllustrate();
           */
        parser.mExplain = mExplain;

        parser.prompt();
        while(!parser.isDone()) {
            parser.parse();
        }

        if (interactive) {
            System.out.println("");
        }
    }

    public boolean isDone() {
        return mDone;
    }

    public void setLoadOnly(boolean loadOnly)
    {
        mLoadOnly = loadOnly;
    }

    private void setBatchOn() {
        mPigOut.setBatchOn();

    }

    @Override
    protected void processFsCommand(String[] arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processHistory(boolean arg0) {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processIllustrate(String arg0, String arg1, String arg2,
            List<String> arg3, List<String> arg4) throws IOException,
              ParseException {
                  // TODO Auto-generated method stub
                  log.info("Not implemented.");
    }

    @Override
    protected void processKill(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processLS(String arg0) throws IOException {
        // TODO Auto-generated method stub
        log.info("Not implemented.");
    }

    @Override
    protected void processMkdir(String arg0) throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void processMove(String arg0, String arg1) throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void processPWD() throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void processRegister(String arg0) throws IOException {
        mPigOut.registerJar(arg0);
    }

    @Override
    protected void processRegister(String path, String scriptingLang, String namespace)
    throws IOException, ParseException {
    //fengshen 10/18/2013 add to handle udf jars
    if(path.endsWith(".jar")) {
        if (scriptingLang != null || namespace != null) {
            throw new ParseException("Cannot register a jar with a scripting language or namespace");
        }
        mPigOut.registerJar(path);
    } else {
        //mPigServer.registerCode(path, scriptingLang, namespace);
    }
    }

    @Override
    protected void processRemove(String arg0, String arg1) throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void processSQLCommand(String arg0) throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void processScript(String arg0, boolean arg1, List<String> arg2,
            List<String> arg3) throws IOException, ParseException {
        log.info("Not implemented.");
    }

    protected void processSet() throws IOException,
              ParseException {
        log.info("Not implemented.");
    }

    @Override
    protected void processSet(String arg0, String arg1) throws IOException,
              ParseException {
    	log.info("Not implemented.");
    }

    @Override
    protected void processShCommand(String[] arg0) throws IOException {
        log.info("Not implemented.");
    }

    @Override
    protected void quit() {
        mDone = true;
    }
}

