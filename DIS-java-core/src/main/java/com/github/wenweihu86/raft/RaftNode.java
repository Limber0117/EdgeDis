package com.github.wenweihu86.raft;

import com.baidu.brpc.client.RpcCallback;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.proto.RaftProto.EntryType;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.raft.util.DatabaseLog;
import com.github.wenweihu86.raft.util.EVENT;
import com.google.protobuf.ByteString;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.apache.tools.ant.taskdefs.condition.And;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.wenweihu86.raft.util.*;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 * 该类是raft核心类，主要有如下功能：
 * 1. saving all core data of the raft nodes, including the node status, logs and snapshots. CN: 保存raft节点核心数据（节点状态信息、日志信息、snapshot等），
 * 2. providing the rpc functions that one raft node sends requests to other raft nodes. CN: raft节点向别的raft发起rpc请求相关函数
 * 3. timers for the raft nodes, including the heart beat signal timer of the leader node, the timer for raising the election. CN: raft节点定时器：主节点心跳定时器、发起选举定时器。
 */
public class RaftNode {
	
	// lots of new ideas can be implemented in this class/file.
	// change the Raft protocol to satisfy the EDI problem, i.e., confirm every nodes can receive the message correctly.
	// calculate the efficiency of each step.

	// in this version, each node has four states, including the Follower, Pre_Candidate, Candidate and Leader.
    public enum NodeState {
        STATE_FOLLOWER,
        STATE_PRE_CANDIDATE,
        STATE_CANDIDATE,
        STATE_LEADER
    }

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    private RaftOptions raftOptions;
    private RaftProto.Configuration configuration;
    private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();
    private RaftProto.Server localServer;
    private StateMachine stateMachine;
    private SegmentedLog raftLog;
    private Snapshot snapshot;

    // By default, each node is set to Follower
    private NodeState state = NodeState.STATE_FOLLOWER;
    
    // the last TERM that current server recored, initialized as 0 and increases by 1 gradually.
    // this is 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private long currentTerm;
    
    // the potential leader node that voted by the current node in the last election.
    // s 在当前获得选票的候选人的Id
    private int votedFor;
    
    // the current leader, might have dead if the election timer is timeout.
    private int leaderId; // leader节点id


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Here, COMMITED means this log has been distributed to majority of nodes                                                                                   //
    // and APPLIED means this log has been executed (store to database in this project) to modify the state machine of each node.  //
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    // the last index of the committed log (values)
    // s已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;

    // the last index of logs that have been applied to the state machine
    //s 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private volatile long lastAppliedIndex;
    
    //Reentrant Locks are provided in Java to provide synchronization with greater flexibility.
    //Reentrant是JAVA中的一种线程间加锁的机制
    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();
    private Condition catchUpCondition = lock.newCondition();

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    /**
     * commented by: BOLI
     * RaftNode constructor function
     * @param raftOptions 配置参数
     * @param servers        集群所有服务器列表 arraylist 类型
     * @param localServer  本机（ip+port+id)
     * @param stateMachine 状态机（log线程）
     */
    public RaftNode(RaftOptions raftOptions,
                    List<RaftProto.Server> servers,
                    RaftProto.Server localServer,
                    StateMachine stateMachine) {
        this.raftOptions = raftOptions;
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : servers) {
            confBuilder.addServers(server);
        }
        configuration = confBuilder.build();

        this.localServer = localServer;
        this.stateMachine = stateMachine;

        // remove the snapshot Comments:BOLI
        // load log and snapshot
        raftLog = new SegmentedLog(raftOptions.getDataDir(), raftOptions.getMaxSegmentFileSize());
        //snapshot = new Snapshot(raftOptions.getDataDir());
        //snapshot.reload();

        currentTerm = raftLog.getMetaData().getCurrentTerm();
        votedFor = raftLog.getMetaData().getVotedFor();
        
        // commitIndex是snapshot中的index和 log中的index的最大值        
        //commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), raftLog.getMetaData().getCommitIndex());
        commitIndex =  raftLog.getMetaData().getCommitIndex();
        
        // remove the snapshot reloading Comment:BOLI
        if(false) {
	        // discard old log entries
	        if (snapshot.getMetaData().getLastIncludedIndex() > 0
	                && raftLog.getFirstLogIndex() <= snapshot.getMetaData().getLastIncludedIndex()) {
	            raftLog.truncatePrefix(snapshot.getMetaData().getLastIncludedIndex() + 1);
	        }
	        // apply state machine
	        RaftProto.Configuration snapshotConfiguration = snapshot.getMetaData().getConfiguration();
	        if (snapshotConfiguration.getServersCount() > 0) {
	            configuration = snapshotConfiguration;
	        }
	        String snapshotDataDir = snapshot.getSnapshotDir() + File.separator + "data";
	        stateMachine.readSnapshot(snapshotDataDir);
	        for (long index = snapshot.getMetaData().getLastIncludedIndex() + 1;
	             index <= commitIndex; index++) {
	            RaftProto.LogEntry entry = raftLog.getEntry(index);
	            if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
	                stateMachine.apply(entry.getData().toByteArray());
	            } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
	                applyConfiguration(entry);
	            }
	        }
        }
        lastAppliedIndex = commitIndex;
    }// end of funciton RaftNode()

    public void init() {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (!peerMap.containsKey(server.getServerId())
                    && server.getServerId() != localServer.getServerId()) {
                Peer peer = new Peer(server);
                peer.setNextIndex(raftLog.getLastLogIndex() + 1);
                peerMap.put(server.getServerId(), peer);
            }
         }

        // init thread pool
        /**
         * @author boli
		 * CorePoolSize, MaximumPoolSize, KeepAliveTime, TimeUnits, ThreadsQueue
         */
        executorService = new ThreadPoolExecutor(
                raftOptions.getRaftConsensusThreadNum(),
                raftOptions.getRaftConsensusThreadNum(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        /**
         * @author boli
         * change the threads pool from 2 to 4 (but not that useful)
         */
        scheduledExecutorService = Executors.newScheduledThreadPool(10);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                //takeSnapshot();
           }
        }, raftOptions.getSnapshotPeriodSeconds(), raftOptions.getSnapshotPeriodSeconds(), TimeUnit.SECONDS);
        // start election
        resetElectionTimer();
        //????? add this code for debug  Comments:BOLI
        //boolean a = replicate("sss".getBytes(),RaftProto.EntryType.ENTRY_TYPE_DATA);
        //System.out.println(a);
    } // end of function init()

    // client set command Cluster leader复制（分发）数据
    // this is used for Leader only
    public boolean replicate(byte[] data, int dataLength,RaftProto.EntryType entryType) {
        lock.lock();
        long newLastLogIndex = 0;
        try {
            if (state != NodeState.STATE_LEADER) {
                LOG.info("In replicate() funciton: I'm not the leader");
                return false;
            }
            // For leader node, tries to receive data and distribute it to other nodes
            // create a new log entry with Term, Type, Data
            RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setType(entryType)
                    .setData(ByteString.copyFrom(data)).build();
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            entries.add(logEntry); // add to arraylist
            newLastLogIndex = raftLog.append(entries);
             //raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex());
            
            //s 开始异步调用向每个结点分发数据
	        DatabaseLog dbLog = new DatabaseLog();
            if(dataLength>0 && entryType==RaftProto.EntryType.ENTRY_TYPE_DATA) { 	// current operation is for real data entry
	            dbLog.sid = localServer.getServerId();
	            dbLog.sip = "leader--all";
	            dbLog.event = EVENT.APPEND_DATA;
	            dbLog.start_time = "";
	            
	
	            long currentTime = System.nanoTime();
	                         
	            for (RaftProto.Server server : configuration.getServersList()) {
	                final Peer peer = peerMap.get(server.getServerId());
	                executorService.submit(new Runnable() {
	                    @Override
	                    public void run() {
	                        try {
								appendEntries(peer,1);
							} catch (ClassNotFoundException | SQLException e) {
								e.printStackTrace();
							} // append current log to all slave edge servers, one-by-one
	                    }
	                });
	            }
	
	            if (raftOptions.isAsyncWrite()) {
	                //s 主节点写成功后，就返回。
	                return true;
	            }
	
	            // sync wait commitIndex >= newLastLogIndex
	            long startTime = System.nanoTime();
	            while (lastAppliedIndex < newLastLogIndex) {
	                if ((System.nanoTime() - startTime)/1000000 >= raftOptions.getMaxAwaitTimeout()) {
	                    break;
	                }
	                commitIndexCondition.await(200000, TimeUnit.NANOSECONDS);
	            }
	            
           
	            dbLog.t_time = (float)(Math.round((System.nanoTime()-currentTime)/10000)/100);
	            float maxTimeConsumption=0;
	            //for (RaftProto.Server server : configuration.getServersList()) {
	                //Peer peer = peerMap.get(server.getServerId());
	                //if(maxTimeConsumption<peer.getExeTime())
	                	//maxTimeConsumption = peer.getExeTime();
	            //}
	            dbLog.end_time = ""+maxTimeConsumption;        
	            dbLog.term = currentTerm;
	            dbLog.log_index = commitIndex;
	            dbLog.data_length = data.length;
	            dbLog.leader = localServer.getServerId();
	            dbLog.detail = "Leader append data entry to all cluster nodes";
	            DataBase.addToDB(dbLog);               
            }else {
            	// send heart beats or configuration
            	for (RaftProto.Server server : configuration.getServersList()) {
	                final Peer peer = peerMap.get(server.getServerId());
	                executorService.submit(new Runnable() {
	                    @Override
	                    public void run() {
	                        try {
								appendEntries(peer,2);
							} catch (ClassNotFoundException | SQLException e) {
								e.printStackTrace();
							} // append current log to all slave edge servers, one-by-one
	                    }
	                });
	            }
	
	            if (raftOptions.isAsyncWrite()) {
	                //s 主节点写成功后，就返回。
	                return true;
	            }
	
	            // sync wait commitIndex >= newLastLogIndex
	            long startTime = System.nanoTime();
	            while (lastAppliedIndex < newLastLogIndex) {
	                if ((System.nanoTime() - startTime)/1000000 >= raftOptions.getMaxAwaitTimeout()) {
	                    break;
	                }
	                commitIndexCondition.await(200000, TimeUnit.NANOSECONDS);
	            }	
            }         
          
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            lock.unlock();
        }
        LOG.debug("lastAppliedIndex={} newLastLogIndex={}", lastAppliedIndex, newLastLogIndex);
        //s 在规定的时间内未能发布到半数以上节点
        if (lastAppliedIndex < newLastLogIndex) {
            return false;
        }
        //s 成功发布到半数以上节点
        return true;
    }

    /**
     * append new log entry to specific server， 这个函数只有leader才能调用
     * @param peer  is the cluster server
     * @param type: 1 is data  2 is heart beats
     * s 这里的log entry既包含心跳包，也包含具体数据包，区别在于心跳包的  entry data 为空
     * @throws SQLException 
     * @throws ClassNotFoundException 
     */
    public void appendEntries(Peer peer, int type) throws ClassNotFoundException, SQLException {
        RaftProto.AppendEntriesRequest.Builder requestBuilder = RaftProto.AppendEntriesRequest.newBuilder();
        long prevLogIndex;
        long numEntries;

        boolean isNeedInstallSnapshot = false;
        //lock.lock();
        //try {
        	//getNextIndex() 返回需要发送给follower的下一个日志条目的索引值，只对leader有效
            //long firstLogIndex = raftLog.getFirstLogIndex();
            // if peer.getNextIndex() < firstLogIndex，说明当前节点没有任何日志，需要根据snapshot重建log数据
            //if (peer.getNextIndex() < firstLogIndex) {
            	// remove the Snapshot process Comments:BOLI
                //isNeedInstallSnapshot = true;
           // }
       // } finally {
       //     lock.unlock();
       // }

        // remove the snapshot process Comment:BOLI
        long lastSnapshotIndex=-5;
        long lastSnapshotTerm=-5;
        
        if(false) {
	        LOG.debug("is need snapshot={}, peer={}", isNeedInstallSnapshot, peer.getServer().getServerId());
	        if (isNeedInstallSnapshot) {
	            if (!installSnapshot(peer)) {
	                return;
	            }
	        }
        
	        //s 获取最后一个snapshot的 term 和index
	        //long lastSnapshotIndex;
	        //long lastSnapshotTerm;
	        snapshot.getLock().lock();
	        try {
	            lastSnapshotIndex = snapshot.getMetaData().getLastIncludedIndex();
	            lastSnapshotTerm = snapshot.getMetaData().getLastIncludedTerm();
	        } finally {
	            snapshot.getLock().unlock();
	        }
        }// end if(false)
        
        // s获取之前条目的term 和index
        lock.lock();
        try {
            long firstLogIndex = raftLog.getFirstLogIndex();
            Validate.isTrue(peer.getNextIndex() >= firstLogIndex);
            prevLogIndex = peer.getNextIndex() - 1;
            long prevLogTerm;
            if (prevLogIndex == 0) {
                prevLogTerm = 0;
            } else if (prevLogIndex == lastSnapshotIndex) {
                prevLogTerm = lastSnapshotTerm;
            } else {
                prevLogTerm = raftLog.getEntryTerm(prevLogIndex);
            }
            requestBuilder.setServerId(localServer.getServerId());
            requestBuilder.setTerm(currentTerm);
            requestBuilder.setPrevLogTerm(prevLogTerm);
            requestBuilder.setPrevLogIndex(prevLogIndex);
            // put all entries to be sent to one request object and return its length to numEntries
            // the following packEntries contains command "requestBuilder.addEntries(entry)" to add all entries sequentially to request object
            numEntries = packEntries(peer.getNextIndex(), requestBuilder);
            requestBuilder.setCommitIndex(Math.min(commitIndex, prevLogIndex + numEntries));
        } finally {
            lock.unlock();
        }
        
        // send out the request (contains data entry)
        RaftProto.AppendEntriesRequest request = requestBuilder.build();
        // receive the response object
        
        //calculate the pure request-response time consumption
        DatabaseLog dbLog = new DatabaseLog();    
    	// current operation is for real data entry
        dbLog.sid = peer.getServer().getServerId();
        dbLog.sip = "leader--edge";
        dbLog.event = EVENT.APPEND_DATA;
        dbLog.start_time = "";
        //peer.setExeTime(0);
        long startTime = System.nanoTime();
        // send a request to remove edge server 
        RaftProto.AppendEntriesResponse response = peer.getRaftConsensusServiceAsync().appendEntries(request);
        //peer.setExeTime((float)(Math.round((System.nanoTime()-startTime)/10000)/100));
        dbLog.end_time = "";        
        dbLog.t_time = (float)(Math.round((System.nanoTime()-startTime)/10000)/100);
        dbLog.detail = "append data entry to one edge server node";
        if(type==1) {
        	DataBase.addToDB(dbLog);   
        }
        LOG.debug("One Request-Response time consumption:{} ms",  (System.nanoTime() - startTime)/1000000);
        
        lock.lock();
        try {
        	// failed to append new node
            if (response == null) {
                //LOG.warn("appendEntries with peer[{}:{}] failed",peer.getServer().getEndpoint().getHost(), peer.getServer().getEndpoint().getPort());
                if (!ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                    peerMap.remove(peer.getServer().getServerId());
                    peer.getRpcClient().stop();
                }
                return;
            }
            // append new node success.
            //LOG.info("AppendEntries response[{}] from server {} " +  "in term {} (my term is {})",response.getResCode(), peer.getServer().getServerId(), response.getTerm(), currentTerm);

            if (response.getTerm() > currentTerm) {
            	//s 有更新的term，则本机的term自动改为新term，leader自动改为follower
                stepDown(response.getTerm());
            } else {
                if (response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
                    peer.setMatchIndex(prevLogIndex + numEntries);
                    peer.setNextIndex(peer.getMatchIndex() + 1);
                    if (ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                        advanceCommitIndex(); // update the leader's status according to whether majority edge servers have received this data(request)
                    } else {
                    	//s 此处节点跟leader的同步程度。
                        //if (raftLog.getLastLogIndex() - peer.getMatchIndex() <= raftOptions.getCatchupMargin()) {
                            //LOG.debug("peer catch up the leader");
                           // peer.setCatchUp(true);
                            // signal the caller thread
                           // catchUpCondition.signalAll();
                        //}
                    }
                } else {
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // in lock
    // stay as follower
    //s 之前一轮投票过程中自己拥护（投票）的candidate没有选票过半，因此真正的leader不是自己vote for的那个，需要更正为 真正的leader
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            LOG.error("can't be happened");
            return;
        }
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = 0;// cancel the leaderID, as the response does not contain the parameter.
            votedFor = 0; // cancel the voteFor value
            raftLog.updateMetaData(currentTerm, votedFor, null, null);
        }
        state = NodeState.STATE_FOLLOWER;
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        resetElectionTimer();
    }

    public void takeSnapshot() {
        if (snapshot.getIsInstallSnapshot().get()) {
            LOG.info("already in install snapshot, ignore take snapshot");
            return;
        }

        snapshot.getIsTakeSnapshot().compareAndSet(false, true);
        try {
            long localLastAppliedIndex;
            long lastAppliedTerm = 0;
            RaftProto.Configuration.Builder localConfiguration = RaftProto.Configuration.newBuilder();
            lock.lock();
            try {
            	
            	//s 判断日志文件是否够大，如果不够大，则不执行 snapshot
                if (raftLog.getTotalSize() < raftOptions.getSnapshotMinLogSize()) {
                    return;
                }
                if (lastAppliedIndex <= snapshot.getMetaData().getLastIncludedIndex()) {
                    return;
                }
                localLastAppliedIndex = lastAppliedIndex;
                if (lastAppliedIndex >= raftLog.getFirstLogIndex()
                        && lastAppliedIndex <= raftLog.getLastLogIndex()) {
                    lastAppliedTerm = raftLog.getEntryTerm(lastAppliedIndex);
                }
                localConfiguration.mergeFrom(configuration);
            } finally {
                lock.unlock();
            }

            boolean success = false;
            snapshot.getLock().lock();
            try {
                LOG.info("start taking snapshot");
                // take snapshot
                String tmpSnapshotDir = snapshot.getSnapshotDir() + ".tmp";
                snapshot.updateMetaData(tmpSnapshotDir, localLastAppliedIndex,
                        lastAppliedTerm, localConfiguration.build());
                String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + "data";
                stateMachine.writeSnapshot(tmpSnapshotDataDir);
                // rename tmp snapshot dir to snapshot dir
                try {
                    File snapshotDirFile = new File(snapshot.getSnapshotDir());
                    if (snapshotDirFile.exists()) {
                        FileUtils.deleteDirectory(snapshotDirFile);
                    }
                    FileUtils.moveDirectory(new File(tmpSnapshotDir),
                            new File(snapshot.getSnapshotDir()));
                    LOG.info("end taking snapshot, result=success");
                    success = true;
                } catch (IOException ex) {
                    LOG.warn("move direct failed when taking snapshot, msg={}", ex.getMessage());
                }
            } finally {
                snapshot.getLock().unlock();
            }

            if (success) {
                //s 重新加载snapshot
                long lastSnapshotIndex = 0;
                snapshot.getLock().lock();
                try {
                    snapshot.reload();
                    lastSnapshotIndex = snapshot.getMetaData().getLastIncludedIndex();
                } finally {
                    snapshot.getLock().unlock();
                }

                // discard old log entries
                lock.lock();
                try {
                    if (lastSnapshotIndex > 0 && raftLog.getFirstLogIndex() <= lastSnapshotIndex) {
                        raftLog.truncatePrefix(lastSnapshotIndex + 1);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.getIsTakeSnapshot().compareAndSet(true, false);
        }
    }

    // in lock
    public void applyConfiguration(RaftProto.LogEntry entry) {  //此处只增加了节点，未减少节点
        try {
        	//////////////////////////////////////////////
        	//// Here import the new configuration  ////  需要仔细研究下面的函数
        	//////////////////////////////////////////////
            RaftProto.Configuration newConfiguration
                    = RaftProto.Configuration.parseFrom(entry.getData().toByteArray());
            configuration = newConfiguration;
            // update peerMap
            for (RaftProto.Server server : newConfiguration.getServersList()) {
            	//s 逐个检查configuration中的节点信息，如果不在当前列表里面，则添加进来
                if (!peerMap.containsKey(server.getServerId())
                        && server.getServerId() != localServer.getServerId()) {
                    Peer peer = new Peer(server);
                    peer.setNextIndex(raftLog.getLastLogIndex() + 1);
                    peerMap.put(server.getServerId(), peer);
                }
            }
            LOG.info("new conf is {}, leaderId={}", jsonFormat.printToString(newConfiguration), leaderId);
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
    }

    public long getLastLogTerm() {
        long lastLogIndex = raftLog.getLastLogIndex();
        //if (lastLogIndex >= raftLog.getFirstLogIndex()) {
            return raftLog.getEntryTerm(lastLogIndex);
       // } else {
            // log为空，lastLogIndex == lastSnapshotIndex
        //    return snapshot.getMetaData().getLastIncludedTerm();
        //}
    }

    /**
     * 选举定时器
     * s 采用两段完成选举，首先 startPreVote()，如果胜出，则在prevote的末尾自动进行 startVote()
     */
    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                	if(localServer.getServerId()==4) {
                		startPreVote();
					}
				} catch (ClassNotFoundException | SQLException e) {
					e.printStackTrace();
				}
            }
        }, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds()
                + random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
        LOG.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    /**
     * 客户端发起pre-vote请求。
     * pre-vote/vote是典型的二阶段实现。
     * 作用是防止某一个节点断网后，不断的增加term发起投票；
     * 当该节点网络恢复后，会导致集群其他节点的term增大，导致集群状态变更。
     * @throws SQLException 
     * @throws ClassNotFoundException 
     */
    private void startPreVote() throws ClassNotFoundException, SQLException {
        lock.lock();
        try {
        	//s 如果当前服务器配置中不含有本节点，则重置选举计时器（取消当前计时）
        	//s 所以新加入的节点会永久循环取消计时器，直到被加入到配置文件中为止，所以新加入节点无法通过选举变为 leader
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            LOG.info("Running pre-vote in term {}", currentTerm);
            //s 变为candidate开始pre-vote
            state = NodeState.STATE_PRE_CANDIDATE;
        } finally {
            lock.unlock();
        }
        // here, the current node start the pre vote for itself
        // add on log to database
        DatabaseLog dbLog = new DatabaseLog();
        dbLog.sid = localServer.getServerId();
        dbLog.sip = localServer.getEndpoint().getHost()+ ":" + localServer.getEndpoint().getPort();
        dbLog.event = EVENT.START_PRE_VOTE;
        dbLog.start_time = " "+(float)(Math.round(System.nanoTime()/10000)/100);
        dbLog.term = currentTerm;
        dbLog.log_index = commitIndex;
        dbLog.detail = "start the pre vote for itself";
        DataBase.addToDB(dbLog);               

        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {
            	//s 自己肯定投自己一票，所以不用发送rpc调用
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    preVote(peer); //s 发送pre-vote RPC调用
                }
            });
        }
        resetElectionTimer(); //s 如果超时，则重投来一次
    }


    /**
     * 客户端发起pre-vote请求
     * @param peer 服务端节点信息
     */
    private void preVote(Peer peer) {
        LOG.info("begin pre vote request");
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            requestBuilder.setServerId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(raftLog.getLastLogIndex())
                    .setLastLogTerm(getLastLogTerm());
        } finally {
            lock.unlock();
        }

        RaftProto.VoteRequest request = requestBuilder.build();
        peer.getRaftConsensusServiceAsync().preVote(
                request, new PreVoteResponseCallback(peer, request));
    }

    
    
    /**
     * 客户端发起正式vote，对candidate有效
     * @throws SQLException 
     * @throws ClassNotFoundException 
     */
    private void startVote() throws ClassNotFoundException, SQLException {
        lock.lock();
        try {
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            currentTerm++; //s 只有正式投票阶段才会 term+1
            LOG.info("Running for election in term {}", currentTerm);
            state = NodeState.STATE_CANDIDATE;
            leaderId = 0;
            votedFor = localServer.getServerId(); //先投自己一票
        } finally {
            lock.unlock();
        }
        
        // here, the current node starts to call for formal vote for itself
        // add on log to database
        DatabaseLog dbLog = new DatabaseLog();
        dbLog.sid = localServer.getServerId();
        dbLog.sip = localServer.getEndpoint().getHost()+ ":" + localServer.getEndpoint().getPort();
        dbLog.event = EVENT.START_FORMAL_VOTE;
        dbLog.start_time = " "+(float)(Math.round(System.nanoTime()/10000)/100);
        dbLog.term = currentTerm;
        dbLog.log_index = commitIndex;
        dbLog.detail = "start the formal vote for itself";
        DataBase.addToDB(dbLog);               
        
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    requestVote(peer);
                }
            });
        }
    }

    /**
     * 客户端发起正式vote请求
     * @param peer 服务端节点信息
     */
    private void requestVote(Peer peer) {
        LOG.info("begin vote request");
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            requestBuilder.setServerId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(raftLog.getLastLogIndex())
                    .setLastLogTerm(getLastLogTerm());
        } finally {
            lock.unlock();
        }

        RaftProto.VoteRequest request = requestBuilder.build();
        peer.getRaftConsensusServiceAsync().requestVote(
                request, new VoteResponseCallback(peer, request));
    }

    private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {
        private Peer peer;
        private RaftProto.VoteRequest request;

        public PreVoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        // Pre vote
        @Override
        public void success(RaftProto.VoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted());
                if (currentTerm != request.getTerm() || state != NodeState.STATE_PRE_CANDIDATE) {
                    LOG.info("ignore preVote RPC result");
                    return;
                }
                // 收到的预投票结果中，至少有一个投票显示别的candidate拥有更高的term
                //这种情况下自己的投票请求已经无用了，所以废弃掉自己的投票流程
                if (response.getTerm() > currentTerm) {
                    LOG.info("Received pre vote response from server {} " +
                                    "in term {} (this server's term was {})",
                            peer.getServer().getServerId(),
                            response.getTerm(),
                            currentTerm);
                    stepDown(response.getTerm()); //收到对方传来的term和vote for，则停止自己的投票申请，改为改用户相同的term
                } else {
                	//s 收到的response.getTerm == currentTerm
                    if (response.getGranted()) {
                        LOG.info("get pre vote granted from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 1;
                        for (RaftProto.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId());
                            if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {
                                voteGrantedNum += 1;
                            }
                        }
                        LOG.info("preVoteGrantedNum={}", voteGrantedNum);
                        // s 得到的选票超过半数
                        int majority = 0;
                        majority = (int)Math.ceil((float)(configuration.getServersCount() +1)/2);
                        if (voteGrantedNum > majority) {
                            LOG.info("get majority pre vote, serverId={} when pre vote, start vote",
                                    localServer.getServerId());
                            
                            // here, the current node start the formal vote for itself
                            // add on log to database
                            DatabaseLog dbLog = new DatabaseLog();
                            dbLog.sid = localServer.getServerId();
                            dbLog.sip = localServer.getEndpoint().getHost()+ ":" + localServer.getEndpoint().getPort();
                            dbLog.event = EVENT.FINISH_PRE_VOTE;
                            dbLog.start_time = " "+(float)(Math.round(System.nanoTime()/10000)/100);
                            dbLog.term = currentTerm;
                            dbLog.log_index = commitIndex;
                            dbLog.detail = "Success in the pre vote";
                            DataBase.addToDB(dbLog);
                            
                            startVote(); //s 预投票过半数才可以开始正式投票
                        }
                    } else {
                        LOG.info("pre vote denied by server {} with term {}, my term is {}",
                                peer.getServer().getServerId(), response.getTerm(), currentTerm);
                    }
                }
            } catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("pre vote with peer[{}:{}] failed",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(new Boolean(false));
        }
    }

    // Vote 真正的投票过程
    private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {
        private Peer peer;
        private RaftProto.VoteRequest request;

        public VoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted());
                if (currentTerm != request.getTerm() || state != NodeState.STATE_CANDIDATE) {
                    LOG.info("ignore requestVote RPC result");
                    return;
                }
                // 收到的投票中含有更高的 term，意味着自己的term已经过期了
                if (response.getTerm() > currentTerm) {
                    LOG.info("Received RequestVote response from server {} " +
                                    "in term {} (this server's term was {})",
                            peer.getServer().getServerId(),
                            response.getTerm(),
                            currentTerm);
                    //s 用户新leader
                    stepDown(response.getTerm());
                } else {
                	// 当 response.getTerm() <= currentTerm时，自己的term是最高的
                	// 此时需要跟同样term的其他server抢leader选票，率先过半的当选
                    if (response.getGranted()) {
                        LOG.info("Got vote from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 0;
                        if (votedFor == localServer.getServerId()) {
                            voteGrantedNum += 1;
                        }
                        for (RaftProto.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId());
                            if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {
                                voteGrantedNum += 1;
                            }
                        }
                        LOG.info("voteGrantedNum={}", voteGrantedNum);
                        
                        //s 因为结点有可能是偶数，所以需要上取整获得majority的数量
                        int majority = 0;
                        majority = (int)Math.ceil((float)(configuration.getServersCount() +1)/2);
                        
                        if (voteGrantedNum >= majority) {
                            LOG.info("Got majority vote, serverId={} become leader", localServer.getServerId());
                            becomeLeader(); //s 正式投票半数通过，则成为leader    
                            

                            // here, the current node start the formal vote for itself
                            // add on log to database
                            DatabaseLog dbLog = new DatabaseLog();
                            dbLog.sid = localServer.getServerId();
                            dbLog.sip = localServer.getEndpoint().getHost()+ ":" + localServer.getEndpoint().getPort();
                            dbLog.event = EVENT.BECOME_LEADER;
                            dbLog.leader = localServer.getServerId(); // it has been selected as the new leader
                            dbLog.start_time = " "+(float)(Math.round(System.nanoTime()/10000)/100);
                            dbLog.term = currentTerm;
                            dbLog.log_index = commitIndex;
                            dbLog.detail = "Become the new leader";
                            DataBase.addToDB(dbLog);                            
                            
                        }
                    } else {
                        LOG.info("Vote denied by server {} with term {}, my term is {}",
                                peer.getServer().getServerId(), response.getTerm(), currentTerm);
                    }
                }
            } catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("requestVote with peer[{}:{}] failed",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(new Boolean(false));
        }
    }

    // in lock
    private void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        startNewHeartbeat();
    }

    // heartbeat timer, append entries
    // in lock
    private void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                startNewHeartbeat();
            }
        }, raftOptions.getHeartbeatPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    // in lock, 开始心跳，对leader有效
    private void startNewHeartbeat() {
        LOG.debug("start new heartbeat, peers={}", peerMap.keySet());
        for (final Peer peer : peerMap.values()) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
						appendEntries(peer,2);
					} catch (ClassNotFoundException | SQLException e) {
						e.printStackTrace();
					}
                }
            });
        }
        resetHeartbeatTimer();
    }

    // in lock, for leader
    private void advanceCommitIndex() {
        //s 获取quorum matchIndex    ss
        int peerNum = configuration.getServersList().size();
        long[] matchIndexes = new long[peerNum];
        int i = 0;
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() != localServer.getServerId()) {
                Peer peer = peerMap.get(server.getServerId());
                matchIndexes[i++] = peer.getMatchIndex();
            }
        }
        matchIndexes[i] = raftLog.getLastLogIndex();
        Arrays.sort(matchIndexes);
        //s 判断已经提交（committed） log是否超过半数
        int majority = (int) Math.ceil((float)(peerNum+1)/2);
        long newCommitIndex = matchIndexes[majority];
        LOG.debug("newCommitIndex={}, oldCommitIndex={}", newCommitIndex, commitIndex);
        if (raftLog.getEntryTerm(newCommitIndex) != currentTerm) {
            LOG.debug("newCommitIndexTerm={}, currentTerm={}",
                    raftLog.getEntryTerm(newCommitIndex), currentTerm);
            return;
        }

        if (commitIndex >= newCommitIndex) {
            return;
        }
        long oldCommitIndex = commitIndex;
        commitIndex = newCommitIndex;
        raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex(), commitIndex);
        //s 同步到状态机ss
        //s 如果收到的是configuration则启动Cluster配置过程，否则，启动数据apply
        // remove the storing data process  Comments:BOLI
        if(false) {
	        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++) {
	            RaftProto.LogEntry entry = raftLog.getEntry(index);
	            if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
	                //stateMachine.apply(entry.getData().toByteArray());
	            } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
	                applyConfiguration(entry);
	            }
	        }
        }
        lastAppliedIndex = commitIndex;
        LOG.debug("commitIndex={} lastAppliedIndex={}", commitIndex, lastAppliedIndex);
        commitIndexCondition.signalAll();
    }

    // in lock
    private long packEntries(long nextIndex, RaftProto.AppendEntriesRequest.Builder requestBuilder) {
        long lastIndex = Math.min(raftLog.getLastLogIndex(),
                nextIndex + raftOptions.getMaxLogEntriesPerRequest() - 1);
        for (long index = nextIndex; index <= lastIndex; index++) {
            RaftProto.LogEntry entry = raftLog.getEntry(index);
            requestBuilder.addEntries(entry);
        }
        return lastIndex - nextIndex + 1;
    }

    private boolean installSnapshot(Peer peer) {
    	// remove the snapshot operation Comments:BOLI
    	//if(true)
    		//return true;
        if (snapshot.getIsTakeSnapshot().get()) {
            LOG.info("already in take snapshot, please send install snapshot request later");
            return false;
        }
        if (!snapshot.getIsInstallSnapshot().compareAndSet(false, true)) {
            LOG.info("already in install snapshot");
            return false;
        }

        LOG.info("begin send install snapshot request to server={}", peer.getServer().getServerId());
        boolean isSuccess = true;
        TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = snapshot.openSnapshotDataFiles();
        LOG.info("total snapshot files={}", snapshotDataFileMap.keySet());
        try {
            boolean isLastRequest = false;
            String lastFileName = null;
            long lastOffset = 0;
            long lastLength = 0;
            while (!isLastRequest) {
                RaftProto.InstallSnapshotRequest request
                        = buildInstallSnapshotRequest(snapshotDataFileMap, lastFileName, lastOffset, lastLength);
                if (request == null) {
                    LOG.warn("snapshot request == null");
                    isSuccess = false;
                    break;
                }
                if (request.getIsLast()) {
                    isLastRequest = true;
                }
                LOG.info("install snapshot request, fileName={}, offset={}, size={}, isFirst={}, isLast={}",
                        request.getFileName(), request.getOffset(), request.getData().toByteArray().length,
                        request.getIsFirst(), request.getIsLast());
                RaftProto.InstallSnapshotResponse response
                        = peer.getRaftConsensusServiceAsync().installSnapshot(request);
                if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
                    lastFileName = request.getFileName();
                    lastOffset = request.getOffset();
                    lastLength = request.getData().size();
                } else {
                    isSuccess = false;
                    break;
                }
            }

            if (isSuccess) {
                long lastIncludedIndexInSnapshot;
                snapshot.getLock().lock();
                try {
                    lastIncludedIndexInSnapshot = snapshot.getMetaData().getLastIncludedIndex();
                } finally {
                    snapshot.getLock().unlock();
                }

                lock.lock();
                try {
                    peer.setNextIndex(lastIncludedIndexInSnapshot + 1);
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            snapshot.closeSnapshotDataFiles(snapshotDataFileMap);
            snapshot.getIsInstallSnapshot().compareAndSet(true, false);
        }
        LOG.info("end send install snapshot request to server={}, success={}",
                peer.getServer().getServerId(), isSuccess);
        return isSuccess;
    }

    private RaftProto.InstallSnapshotRequest buildInstallSnapshotRequest(
            TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap,
            String lastFileName, long lastOffset, long lastLength) {
        RaftProto.InstallSnapshotRequest.Builder requestBuilder = RaftProto.InstallSnapshotRequest.newBuilder();

        snapshot.getLock().lock();
        try {
            if (lastFileName == null) {
                lastFileName = snapshotDataFileMap.firstKey();
                lastOffset = 0;
                lastLength = 0;
            }
            Snapshot.SnapshotDataFile lastFile = snapshotDataFileMap.get(lastFileName);
            long lastFileLength = lastFile.randomAccessFile.length();
            String currentFileName = lastFileName;
            long currentOffset = lastOffset + lastLength;
            raftOptions.getSnapshotPeriodSeconds();
            raftOptions.getSnapshotPeriodSeconds();
            raftOptions.getMaxSnapshotBytesPerRequest();
         
            int currentDataSize =0;
            currentDataSize =  raftOptions.getMaxSnapshotBytesPerRequest();
                        
            Snapshot.SnapshotDataFile currentDataFile = lastFile;
            
            if (lastOffset + lastLength < lastFileLength) {
                if (lastOffset + lastLength + raftOptions.getMaxSnapshotBytesPerRequest() > lastFileLength) {
                    currentDataSize = (int) (lastFileLength - (lastOffset + lastLength));
                }
            } else {
                Map.Entry<String, Snapshot.SnapshotDataFile> currentEntry
                        = snapshotDataFileMap.higherEntry(lastFileName);
                if (currentEntry == null) {
                    LOG.warn("reach the last file={}", lastFileName);
                    return null;
                }
                currentDataFile = currentEntry.getValue();
                currentFileName = currentEntry.getKey();
                currentOffset = 0;
                int currentFileLenght = (int) currentEntry.getValue().randomAccessFile.length();
                if (currentFileLenght < raftOptions.getMaxSnapshotBytesPerRequest()) {
                    currentDataSize = currentFileLenght;
                }
            }
            byte[] currentData = new byte[currentDataSize];
            currentDataFile.randomAccessFile.seek(currentOffset);
            currentDataFile.randomAccessFile.read(currentData);
            requestBuilder.setData(ByteString.copyFrom(currentData));
            requestBuilder.setFileName(currentFileName);
            requestBuilder.setOffset(currentOffset);
            requestBuilder.setIsFirst(false);
            if (currentFileName.equals(snapshotDataFileMap.lastKey())
                    && currentOffset + currentDataSize >= currentDataFile.randomAccessFile.length()) {
                requestBuilder.setIsLast(true);
            } else {
                requestBuilder.setIsLast(false);
            }
            if (currentFileName.equals(snapshotDataFileMap.firstKey()) && currentOffset == 0) {
                requestBuilder.setIsFirst(true);
                requestBuilder.setSnapshotMetaData(snapshot.getMetaData());
            } else {
                requestBuilder.setIsFirst(false);
            }
        } catch (Exception ex) {
            LOG.warn("meet exception:", ex);
            return null;
        } finally {
            snapshot.getLock().unlock();
        }

        lock.lock();
        try {
            requestBuilder.setTerm(currentTerm);
            requestBuilder.setServerId(localServer.getServerId());
        } finally {
            lock.unlock();
        }

        return requestBuilder.build();
    }

    public Lock getLock() {
        return lock;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public SegmentedLog getRaftLog() {
        return raftLog;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftProto.Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(RaftProto.Configuration configuration) {
        this.configuration = configuration;
    }

    public RaftProto.Server getLocalServer() {
        return localServer;
    }

    public NodeState getState() {
        return state;
    }

    public ConcurrentMap<Integer, Peer> getPeerMap() {
        return peerMap;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Condition getCatchUpCondition() {
        return catchUpCondition;
    }
}
