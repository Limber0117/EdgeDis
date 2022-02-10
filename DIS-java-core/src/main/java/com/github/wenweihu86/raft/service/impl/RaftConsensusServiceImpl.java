package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.proto.RaftProto.Endpoint;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.*;
import com.github.wenweihu86.raft.util.*;

//s 跟投票相关的操作都在这个文件里面

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private static final JsonFormat PRINTER = new JsonFormat();

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    
    // 对 preVote请求作出回应
    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false); // 默认不投票( 拒绝投票)
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            //s 如果请求投票者的id不在系统配置中
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build(); //s 直接返回默认值--> setGranted(false)  拒绝投票
            }
            //s 如果请求投票者的term小于自身的term，说明对方太老，立刻返回 false 拒绝投票
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            //s 请求投票者的term up-to-date
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
                  
            
            //s 满足 preVote的条件：
            // 1)s  请求节点的请求term足够新 >=当前term
            // 2)s  请求节点的log足够新  committed term>= current term, LastLogindex >= current LogIndex
            if (!isLogOk) {
            	//s 不满足判定条件，直接返回false 拒绝投票
                return responseBuilder.build();
            } else {
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm()); //s 不管成功与否，都返回投票者当前term值，有啥用？？？？
            }
            LOG.info("preVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    //s 对Vote请求作出回应
    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false); //s  默认先拒绝投票
            responseBuilder.setTerm(raftNode.getCurrentTerm());//s 初始化返回值中节点当前term
            //s 如果请求节点未在系统列表中，则拒绝投票,直接返回 setGranted(false)
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            //s 请求过于老旧，返回false
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            //s 请求足够新，则更新自己的term，追随这个新term
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                // setpDown的作用是 是当前节点voteFor=0 LeaderID=0 term保持最新
                raftNode.stepDown(request.getTerm());
            }
            //s 判定是否为对方投票的条件：
            //s  1. 对方请求投票的term 至少等于节点自身的term，而且随着收到投票邀请的增加，节点自身的term会持续增加
            //s  2. 对方已经提交committed的log，不能比节点自身的旧，需要>=节点自身的数据
            
            //s  ??? 此处还是无法拒绝节点多次投票?????
            
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            //s 只要请求方的term足够新，一定能够获得投票
            //s  如果请求节点的term等于当前节点的term，而且当前节点已经为其他相同term的节点投过票了，则不能再次投票
            //s 但是节点可以为拥有更大term的节点持续投票
            if (raftNode.getVotedFor() == 0 && logIsOk) {
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getServerId());
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null, null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("RequestVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    //s this function is used by the follower edge servers to receive the request and append the data units
    //s 这个函数被普通edge server调用，用来接收leader发送过来的数据或者心跳包
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) throws ClassNotFoundException, SQLException {
        raftNode.getLock().lock();
        try {

        	
            RaftProto.AppendEntriesResponse.Builder responseBuilder
                    = RaftProto.AppendEntriesResponse.newBuilder();
            //s 初始化 默认节点写数据不成功，返回当期term和lastLogIndex
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
            //s 数据包term太小，拒绝
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            
            //s 当收到term>=当前的时候，当前节点term也相应修改
            raftNode.stepDown(request.getTerm());
            // after the stepDown(), current raftNode.getLeaderId()==0 and raftNode.votedFor==0;
            
            //s 如果当前节点经过stepDown()获得现在的term（eg.之前term小于leader的term)
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(), 
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
            //s 如果当前term曾经记录过另外一个leader
            if (raftNode.getLeaderId() != request.getServerId()) {
               // LOG.warn("Another peer={} declares that it is the leader " + "at term={} which was occupied by leader={}",request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                //s 出现了双leader， 返回信息让leader节点的term加1，准备下一次投票？？？
                //s 当前term出现了两个不同的leader，这是所有节点自动term+1，这会导致此节点无法收到任何心跳包和任何数据包
                //s 因此很快就会启动自己的leader 重选机制，变为candidate
                //s 同时，返回的 term也会使当前leader失效
                raftNode.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build(); //s 返回新term，强制当前leader失效
            }

            //s 如果leader的日志跟自己的日志不是连续的，则返回错误信息（Raft要求日志连续）
            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
                //LOG.info("Rejecting AppendEntries RPC would leave gap, " +"request prevLogIndex={}, my lastLogIndex={}",  request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build();
            }
            //s 对leader的数据包进行简单校验，看看上一条已经存储的日志是否与leader相同，若不同则返回false
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex())
                    != request.getPrevLogTerm()) {
                //LOG.info("Rejecting AppendEntries RPC: terms don't agree, " +  "request prevLogTerm={} in prevLogIndex={}, my is {}",  request.getPrevLogTerm(), request.getPrevLogIndex(), raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                Validate.isTrue(request.getPrevLogIndex() > 0);
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                return responseBuilder.build();
            }

            //s 当前数据包不含有任何data，应为心跳包
            if (request.getEntriesCount() == 0) {
               // LOG.debug("heartbeat request from peer={} at term={}, my term={}", request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
                advanceCommitIndex(request);
                return responseBuilder.build();
            }
        	////////////// Begin to add record to database ///////////
        	
//        	DatabaseLog dbLog = new DatabaseLog();
//        	Endpoint endpoint = raftNode.getLocalServer().getEndpoint();
//        	dbLog.sid = raftNode.getLocalServer().getServerId();
//        	dbLog.sip = endpoint.getHost() + ":" + endpoint.getPort();
//        	dbLog.event = EVENT.APPEND_DATA;
//        	long currentTime = System.nanoTime();
//        	dbLog.start_time = "";
        	
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
                index++;
                if (index < raftNode.getRaftLog().getFirstLogIndex()) {
                    continue;
                }
                if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
                        continue;
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);
                }
                entries.add(entry);
                //////////////////////////////////////
                //s           此处可以保存文件了           //
                //////////////////////////////////////
            }
            raftNode.getRaftLog().append(entries);
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            advanceCommitIndex(request);
            
//            ////// successfully append all entries to current node /////
//            dbLog.end_time = "";
//            dbLog.t_time = (float)(Math.round((System.nanoTime()-currentTime)/10000)/100);
//            dbLog.data_length = request.getEntriesCount();
//            dbLog.term = request.getTerm();
//            dbLog.log_index = raftNode.getRaftLog().getLastLogIndex();
//            dbLog.leader = raftNode.getLeaderId();
//            dbLog.detail = "Append new data to current node";
//            /////// add operation log to database ///////
//            DataBase.addToDB(dbLog);
            
            //LOG.info("AppendEntries request from server {} " + "in term {} (my term is {}), entryCount={} resCode={}", request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) {
            LOG.warn("alreay in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build();
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst()) {
                if (file.exists()) {
                    file.delete();
                }
                file.mkdir();
                LOG.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                        request.getSnapshotMetaData().getLastIncludedIndex(),
                        request.getSnapshotMetaData().getLastIncludedTerm(),
                        request.getSnapshotMetaData().getConfiguration());
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";
            File currentDataDir = new File(currentDataDirName);
            if (!currentDataDir.exists()) {
                currentDataDir.mkdirs();
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            //s 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {
                currentDataFile.getParentFile().mkdirs();
            }
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            LOG.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            LOG.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
            raftNode.getSnapshot().getLock().unlock();
        }

        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data";
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            //s 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            } finally {
                raftNode.getLock().unlock();
            }
            LOG.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        }

        return responseBuilder.build();
    }

    // in lock, for follower
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.min(request.getCommitIndex(),
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getRaftLog().updateMetaData(null,null, null, newCommitIndex);
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) {
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
                if (entry != null) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        raftNode.applyConfiguration(entry);
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
