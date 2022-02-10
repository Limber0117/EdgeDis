package com.github.wenweihu86.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusServiceAsync;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {
    private RaftProto.Server server;
    private RpcClient rpcClient;
    private RaftConsensusServiceAsync raftConsensusServiceAsync;
    //s 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // s 已复制日志的最高索引值
    private long matchIndex;
    //s 是否授权当前节点组织投票（in preVote stage）/获得投票（in Vote stage）
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;
    private float exeTime=0;

    public Peer(RaftProto.Server server) {
        this.server = server;

        RpcClientOptions rpcClientOptions = new RpcClientOptions();
        rpcClientOptions.setGlobalThreadPoolSharing(true);
        // here, we can reset the Baidu RPC options.
        // remove the following two lines can used the default settings.
        // changed by BOLI
        rpcClientOptions.setSendBufferSize(16*1024*1024);
        rpcClientOptions.setReceiveBufferSize(16*1024*1024);
        rpcClientOptions.setReadTimeoutMillis(2000);
        rpcClientOptions.setWriteTimeoutMillis(2000);
        rpcClientOptions.setConnectTimeoutMillis(360000);
        
        this.rpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()),rpcClientOptions);
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }
    

    public RpcClient createClient() {
    	return new RpcClient(new Endpoint(server.getEndpoint().getHost(),server.getEndpoint().getPort()));
    }

    public RaftProto.Server getServer() {
        return server;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public RaftConsensusServiceAsync getRaftConsensusServiceAsync() {
        return raftConsensusServiceAsync;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
    
    public void setExeTime(float x) {
    	exeTime = x;
    }
    
    public float getExeTime() {
    	return exeTime;
    }
    
}
