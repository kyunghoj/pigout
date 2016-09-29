package edu.buffalo.cse.pigout.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;


public class PigOutStorage extends PigStorage {
	public static boolean g_all_flushed = false; 

	static PipedOutputStream s_writeEnd = null;
	static PipedInputStream s_readEnd = null;
	static String s_remoteFilename = null;

	private final Log LOG = LogFactory.getLog(getClass());
	
//	private String m_filename; // property filename
	private int m_no_writes;
	private static int m_previous_no_of_writes = 0;
	private String m_private_key;
	private String m_dest;
	private int m_port_no;
	private String m_hostname;
	private String m_username;
	private ScheduledExecutorService activityLogger = null;

	private static Thread remoteCpy_th; 
	private static final int LOGGING_INTERVAL = 500; 
	
	public PigOutStorage(String hostname, String port_no, String dest_path, String username, String private_key) {
		super();
		this.m_hostname = hostname;
		this.m_port_no = Integer.parseInt(port_no);
		this.m_dest = dest_path;
		this.m_username = username;
		this.m_private_key = private_key;
		this.m_no_writes = 0;
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) {
	//	LOG.info("In MyPigStorageSshChunk Cleanup!");
		try {
			if (s_writeEnd != null) return;
			/*
			 * Clean up close all open ssh connections 
			 */
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			//	LOG.info("Exception in thread wait");
				e.printStackTrace();
			}
			
			JSch jsch = new JSch();
			jsch.addIdentity(m_private_key);
			Session session;
	
			session = jsch.getSession(m_username, m_hostname, 22);
	
			MyUserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.connect(30000);
			String command = "~/cleanup.sh " + "\"" + m_dest + "\"";
	
			Channel channel = session.openChannel("exec");
			((ChannelExec)channel).setCommand(command);
			((ChannelExec)channel).setErrStream(System.err);
			channel.connect(10000);
			
			while (channel.isClosed() || channel.isEOF()) {
				LOG.info("exit-status: " + channel.getExitStatus());
				break;
			}
			
			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			LOG.info("Received an Exeption in Cleanup!");
			e.printStackTrace();
		}
		LOG.info("Exiting main thread");
	}

	@Override
	public void putNext(Tuple f) {
		String str = null;
		if (m_no_writes == 0) {
			/* Code for logging every 0.5 second */
		//	activityLogger = Executors.newSingleThreadScheduledExecutor();
			
			/* Try to execute at every 500 ms interval */
			/*
			long initial_delay = LOGGING_INTERVAL - (System.currentTimeMillis() % LOGGING_INTERVAL);
			activityLogger.scheduleAtFixedRate(new Runnable() {
			  @Override
			  public void run() {
				  LOG.info("Numbed of writes in the previous" + LOGGING_INTERVAL + "ms = " + (m_no_writes - m_previous_no_of_writes));
				  m_previous_no_of_writes = m_no_writes;
			  }
			}, initial_delay, LOGGING_INTERVAL, TimeUnit.MILLISECONDS); // command, initialDelay, period, unit
			*/
			/* End for  timed logs */
			
			g_all_flushed = false;
			s_readEnd = new PipedInputStream();
			s_writeEnd = new PipedOutputStream();
			s_remoteFilename = PigMapReduce.sJobConfInternal.get().get("mapred.task.id");
			try {
				s_writeEnd.connect(s_readEnd);
			} catch (IOException e) {
			//	LOG.error("IOException: ");
				e.printStackTrace();
			}
			String full_name = m_dest + "/" + s_remoteFilename + "_" + (int)(Math.random()*100000); //Append a 5 digit Random number
			remoteCpy_th = new Thread(new RemoteCopyThread(s_readEnd, m_hostname, m_port_no, 
						full_name, m_username, m_private_key));
			remoteCpy_th.start();
		}
		
		try {
			str = f.toDelimitedString("\t");
		} catch (ExecException e1) {
		//	LOG.error("Failed to execute an external program.");
			LOG.info("ERROR while converting tuple, tuple is null");
			e1.printStackTrace();
			return;
		}
		str = str + '\n';
		try {
			writer.write(null, f);
			m_no_writes++;
			s_writeEnd.write(str.getBytes());
			
			s_writeEnd.flush();
			if ((m_no_writes  % 500) == 0) {
				LOG.info("Wrote chunk number: " + m_no_writes);
			}
		} catch (Exception e) {
		//	LOG.error("Failed to write: ");
			LOG.info("FAILED TO WRITE");
			e.printStackTrace();
		} 
	}
}

class RemoteCopyThread implements Runnable {
	private final Log LOG = LogFactory.getLog(getClass());
	
	static final String HADOOP = "/usr/local/hadoop/bin/hadoop"; // /opt/hadoop/hadoop-1.2.1/bin/hadoop";
	
	static final String PUT_CMD = "dfs -put -";
	
	static final String HADOOP_PUT = "/usr/local/hadoop/bin/hadoop dfs -put - ";

	static final int PORT_NO = 22;

	String m_hostname;
	String m_hdfs_dest;
	int m_hdfs_port;
	String m_private_key;
	String m_username;

	PipedInputStream m_readEnd;

	public RemoteCopyThread(PipedInputStream readEnd, String hostname, 
			int port_no, String dest_path, String username, String private_key) {
		
		this.m_readEnd = readEnd;
		this.m_hostname = hostname;
		this.m_hdfs_port = port_no;
		this.m_hdfs_dest = dest_path;
		this.m_private_key = private_key;
		this.m_username = username;
	}
	
	@Override
	public void run() {
		try {
			copyTo();
			m_readEnd.close();
			
			//LOG.info("Done, exiting writer");
			return;
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (JSchException e2) {
			e2.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws JSchException 
	 * @throws IOException 
	 */
	public void copyTo() throws JSchException, IOException {
		JSch jsch = new JSch();
		jsch.addIdentity(m_private_key);
		Session session;

		session = jsch.getSession(m_username, m_hostname, PORT_NO);

		MyUserInfo ui = new MyUserInfo();
		session.setUserInfo(ui);
		session.connect(30000);
		/* What is a reasonable time out? */
		//session.connect();
		//session.

	//	String command = HADOOP_PUT + " hdfs://" + m_hostname + ":" + 
	//				m_hdfs_port + m_hdfs_dest ;
		String command = HADOOP_PUT + m_hdfs_dest ;
	//	String command = "cat >> " + m_hdfs_dest ;

		Channel channel = session.openChannel("exec");
		((ChannelExec)channel).setCommand(command);
		channel.setInputStream(m_readEnd);
		((ChannelExec)channel).setErrStream(System.err);
		InputStream in = channel.getInputStream();
		channel.connect(10000);
//		channel.co
		//channel.`
		//LOG.info("Waiting for input...");
		waitForOutput(in, channel);
		//LOG.info("All done");
		channel.disconnect();
		session.disconnect();
		PigOutStorage.g_all_flushed = true;
	}

	private void waitForOutput(InputStream in, Channel channel) throws IOException {
		byte[] tmp = new byte[1024];
		while (true) {
			while (in.available() > 0) {
				int i = in.read(tmp, 0, 1024);
				if (i < 0) {
					break;
				}
				System.out.print(new String(tmp, 0, i));
			}
			if (channel.isClosed() || channel.isEOF()) {
				LOG.info("exit-status: " + channel.getExitStatus());
				break;
			}
			try {
				LOG.info("Sleeping");
				Thread.sleep(1000);
			} catch (Exception ee) {
				ee.printStackTrace();
			}
		}
	}
}

class MyUserInfo implements UserInfo {

	@Override
	public String getPassphrase() {	return null; }

	@Override
	public String getPassword() { return null; }

	@Override
	public boolean promptPassphrase(String arg0) { return true; }

	@Override
	public boolean promptPassword(String arg0) { return true; }

	@Override
	public boolean promptYesNo(String arg0) { return true;}

	@Override
	public void showMessage(String arg0) {}
}
