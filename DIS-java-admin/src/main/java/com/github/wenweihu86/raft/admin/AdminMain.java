package com.github.wenweihu86.raft.admin;

import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.util.*;
import  com.github.wenweihu86.raft.util.EVENT;
import com.github.wenweihu86.raft.util.DatabaseLog;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang3.Validate;
import com.github.wenweihu86.raft.service.RaftClientServiceProxy;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class AdminMain {
    private static final JsonFormat jsonFormat = new JsonFormat();

    public static void main(String[] args) throws Exception {
        // parse args
        if (args.length >2) {
            System.out.println("java -jar AdminMain cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers =  "list://"+ServerList.getServerListWithoutId();
        String cmd = args[0];
        RaftClientService client = new RaftClientServiceProxy(servers);
        // return Leader's ID
        if (cmd.equalsIgnoreCase("getLeader")) {
            RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
            RaftProto.GetConfigurationResponse response = client.getConfiguration(request);
            if (response != null) {
            	System.out.println("The current leader is: \t"+response.getLeader().getServerId());
            } else {
                System.out.printf("response == null");
            }
        }
        
        // return Curernt Clusters
        if (cmd.equalsIgnoreCase("getCluster")) {
            RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
            RaftProto.GetConfigurationResponse response = client.getConfiguration(request);
            if (response != null) {
            	System.out.println("The current cluster: \n"+jsonFormat.printToString(response));
            } else {
                System.out.printf("response == null");
            }
        }      
        
        // add a break line in the database
        if (cmd.equalsIgnoreCase("newExp")) {
            String subCmd = args[1];
        	DataBase.addDBBreak(subCmd);
        } 
        
        
        ((RaftClientServiceProxy) client).stop();
    }
    
    /////////// backup of main() function /////////////////////////////
    public static void mainBackup(String[] args) throws Exception {
        // parse args
        if (args.length < 2) {
            System.out.println("java -jar AdminMain cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers = "list://" + ServerList.getServerListWithoutId();
        String cmd = args[0];
        String subCmd = args[1];
        Validate.isTrue(cmd.equals("conf"));
        Validate.isTrue(subCmd.equals("get")|| subCmd.equals("add")|| subCmd.equals("del"));
        RaftClientService client = new RaftClientServiceProxy(servers);
        if (subCmd.equals("get")) {
            RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
            RaftProto.GetConfigurationResponse response = client.getConfiguration(request);
            if (response != null) {
                System.out.println(jsonFormat.printToString(response));
            } else {
                System.out.printf("response == null");
            }

        } else if (subCmd.equals("add")) {

	        long elapsed = 0L;
	        long start = System.nanoTime();	 
	        
            List<RaftProto.Server> serverList = parseServers(args[2]);
            RaftProto.AddPeersRequest request = RaftProto.AddPeersRequest.newBuilder().addAllServers(serverList).build();
            RaftProto.AddPeersResponse response = client.addPeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }
    		// output the time consumption
	        elapsed = (System.nanoTime() - start)/1000000;
	        System.out.println("Time for adding nodes: \t"+ elapsed + "\t milliseconds");
	        

	        //s 运行结果保存到数据库
	        DatabaseLog dbLog = new DatabaseLog();
	        dbLog.sid = 0;
	        dbLog.sip = "Admin";
	        dbLog.event = EVENT.APPEND_NODE;
	        dbLog.t_time = elapsed;
	        //s 保存删掉的节点的个数
	        dbLog.detail = "append nodes:" + serverList.size();
	        DataBase.addToDB(dbLog);
	        
        } else if (subCmd.equals("del")) {

	        long elapsed = 0L;
	        long start = System.nanoTime();	 
            List<RaftProto.Server> serverList = parseServers(args[3]);
            RaftProto.RemovePeersRequest request = RaftProto.RemovePeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            RaftProto.RemovePeersResponse response = client.removePeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }

    		// output the time consumption
	        elapsed = (System.nanoTime() - start)/1000000;
	        System.out.println("Time for deleting nodes: \t"+ elapsed + "\t milliseconds");
	        
	        //s 运行结果保存到数据库
	        DatabaseLog dbLog = new DatabaseLog();
	        dbLog.sid = 0;
	        dbLog.sip = "Admin";
	        dbLog.event = EVENT.REMOVE_NODE;
	        dbLog.t_time = elapsed;
	        //s 保存删掉的节点的个数
	        dbLog.detail = "delete nodes:" + serverList.size();
	        DataBase.addToDB(dbLog);
	        
        }
        ((RaftClientServiceProxy) client).stop();
    }

    public static List<RaftProto.Server> parseServers(String serversString) {
        List<RaftProto.Server> serverList = new ArrayList<>();
        String[] splitArray1 = serversString.split(",");
        for (String addr : splitArray1) {
            String[] splitArray2 = addr.split(":");
            RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                    .setHost(splitArray2[0])
                    .setPort(Integer.parseInt(splitArray2[1])).build();
            RaftProto.Server server = RaftProto.Server.newBuilder()
                    .setEndpoint(endPoint)
                    .setServerId(Integer.parseInt(splitArray2[2])).build();
            serverList.add(server);
        }
        return serverList;
    }
}
