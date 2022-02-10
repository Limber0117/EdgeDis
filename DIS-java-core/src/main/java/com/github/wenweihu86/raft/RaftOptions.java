package com.github.wenweihu86.raft;


import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.github.wenweihu86.raft.util.*;

/**
 * raft配置选项
 * Created by wenweihu86 on 2017/5/2.
 */
@Setter
@Getter
public class RaftOptions {
	
	// add a construction function to read initial parameters for Raft
	public  RaftOptions() throws SQLException {
		Connection conn = DataBase.DBConnection();
        try (Statement stmt = conn.createStatement()) {
            //execute query
        	String sqlString = "select paraName, paraValue from Params ";
        	ResultSet rs = stmt.executeQuery(sqlString);
        	while (rs.next())
            {             
        		if(rs.getString("paraName").equalsIgnoreCase("electionTimeoutMilliseconds")) {
        			electionTimeoutMilliseconds=rs.getInt("paraValue");
        		};  
        		
        		if(rs.getString("paraName").equalsIgnoreCase("heartbeatPeriodMilliseconds")) {
        			heartbeatPeriodMilliseconds=rs.getInt("paraValue");
        		};  
        		if(rs.getString("paraName").equalsIgnoreCase("maxAwaitTimeout")) {
        			maxAwaitTimeout=rs.getInt("paraValue");
        		};
            }
        	stmt.close();
        }
		conn.close();		
	}
	

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    private int electionTimeoutMilliseconds = 2500;

    // A leader sends RPCs at least this often, even if there is no data to send
    private int heartbeatPeriodMilliseconds =1000;

    // snapshot timeout period, one hour
    private int snapshotPeriodSeconds = 3600;
    
    // do not conduct the snapshot until the size of log entry exceeds snapshotMinLogSize
    // 100 MB
    private int snapshotMinLogSize = 1000 * 1024 * 1024;

    private int maxLogEntriesPerRequest = 2; //At most 5000 log entities in each request.


    // each snapshot transmission can up to 5MB
    private int maxSnapshotBytesPerRequest = 500*1024 * 1024; 
    
    
    //the maximum size of a single segment file, default is 1000MB
    private int maxSegmentFileSize = 1000 * 1024 * 1024;

    // the following is used for preventing those nodes with out-dated data from participating the election.
    // if its data is too old, it even cannot be a candidate
    // catchupMargin means the log differences between current node and its leader
    private long catchupMargin = 50;

    // the size of the threats pool that used for synchronizing with other nodes and leader election.
    private int raftConsensusThreadNum = 200;

    // the maximum await time for replication, unit is ms
    // the results will return if the time consumption is larger than this threshold.
    // it should be enough for most of the cases, as most of them are less than 200ms in practice.
    private long maxAwaitTimeout = 2000;

    //Whether write the data asynchronous. 
    // true 表示主节点保存后就返回，然后异步同步给从节点；
    // false 表示主节点同步给大多数从节点后才返回。
    // false is more safe
    private boolean asyncWrite = false;

    // raft的log和snapshot父目录，绝对路径
    private String dataDir = System.getProperty("com.github.wenweihu86.raft.data.dir");
    

    public void setDataDir(String dataPath) {
    	dataDir = System.getProperty(dataPath);
    }
    
    public String getDataDir() {
    	return dataDir;
    } 

    public void setSnapshotMinLogSize(int size){
    	snapshotMinLogSize = size;
    }
    
    public void setSnapshotPeriodSeconds(int timer){
    	snapshotPeriodSeconds = timer;
    }
    
    public void setMaxSegmentFileSize(int size){
    	maxSegmentFileSize = size;
    }
    
    public int getMaxSegmentFileSize() {
    	return maxSegmentFileSize;
    }

	public int getMaxSnapshotBytesPerRequest() {
		return this.maxSnapshotBytesPerRequest;
	}
	
    public long getMaxAwaitTimeout() {
    	return maxAwaitTimeout;
    }
    
    public int getElectionTimeoutMilliseconds() {
    	return electionTimeoutMilliseconds;
    }
    
    public int getHeartbeatPeriodMilliseconds() {
    	return heartbeatPeriodMilliseconds;
    }
    
    public int getMaxLogEntriesPerRequest() {
    	return maxLogEntriesPerRequest;
    }
    
       
    public long getCatchupMargin() {
    	return catchupMargin;
    }
    
    public int getSnapshotMinLogSize() {
    	return snapshotMinLogSize;
    }
    
    public int getSnapshotPeriodSeconds() {
    	return snapshotPeriodSeconds;
    }
    
    public int getRaftConsensusThreadNum() {
    	return raftConsensusThreadNum;
    }
    
    public boolean isAsyncWrite() {
    	return asyncWrite;
    }

	public void setMaxAwaitTimeout(long maxAwaitTimeout) {
		this.maxAwaitTimeout =this.maxAwaitTimeout+ 1;
		this.maxAwaitTimeout = maxAwaitTimeout;
	}
        
}