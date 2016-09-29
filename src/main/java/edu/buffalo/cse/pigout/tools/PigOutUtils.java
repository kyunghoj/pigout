package edu.buffalo.cse.pigout.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class PigOutUtils {

    private final static Log log = LogFactory.getLog(PigOutUtils.class);

    public static String printLogicalPlanToString(LogicalPlan plan)
        throws FrontendException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        new LogicalPlanPrinter(plan, ps).visit();
        return baos.toString();
    }

    // TODO: Find data information from a catalog, e.g., text file
    // before contacting HDFS
    /** 
     * Returns the size of a file in HDFS in bytes. 
     * 
     * @param   path      a path in HDFS 
     * @param   namenode  namenode of HDFS cluster
     */
	public static long getHDFSFileSize(String path, String namenode) {

		// TODO: We may want to read from a per-cluster 
		// conf file (e.g., hawaii-hadoop-conf.xml)
		Configuration conf = new Configuration();
		conf.set("fs.default.name", namenode);
        long sum = 1;

        try {
            // Initialize new abstract Hadoop FileSystem
            FileSystem fs = FileSystem.get(conf);

            // Specify File path in Hadoop DFS
            Path filenamePath = new Path(path);

            if ( !fs.exists(filenamePath) ) {
            	log.error(path + " does not exist.");
                return -1;
            }

            FileStatus fstat;
            fstat = fs.getFileStatus(filenamePath);

            if (fstat.isDir()) {
                // total size. recursive?
                // make this compilable. recursive case later.
                FileStatus[] fstats = fs.listStatus(filenamePath);
                sum = 0;
                for (FileStatus stat : fstats) {
                    sum += stat.getLen();
                }
                return sum;
            } else {
                return fstat.getLen();
            }
        } catch (IOException ie) {
            log.error("Cannot connect to " + namenode);
            log.error("So just return filesize: " + sum);
        }

        return sum;
	}

    public static String getTimestamp(int offset) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, offset);
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		return formatter.format(cal.getTime());	
	}

}

