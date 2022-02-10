package com.github.wenweihu86.raft.util;

import java.sql.*;

public class ServerList {
		

	/** 
	 * @return String like "136.186.108.120:8055:5,136.186.108.120:8056:6"
	 * @throws SQLException 
	 */
	public static String getServerListWithId() throws SQLException {		
        Connection conn = DataBase.DBConnection();
        String serverList = null;
		// create a Statement
        try (Statement stmt = conn.createStatement()) {
            //execute query
        	String sqlString = "select server, serverid from Servers where status=1 order by serverid asc";
        	ResultSet rs = stmt.executeQuery(sqlString);
        	while (rs.next())
            {             
        		String serverAddress = rs.getString("server");
        		int serverId = rs.getInt("serverid");
        		if(serverList==null) {
        			serverList = serverAddress + ":" + serverId;
        		}
        		else {
        			serverList = serverList + "," + serverAddress + ":" + serverId;
        		}
            }
        	stmt.close();
        }
		conn.close();		
		//System.out.println(serverList);
		return serverList;
		
	}
	
	/**
	 *  return a string like "136.186.108.120:8055,136.186.108.120:8056" without server's id
	 * @return  String like "136.186.108.120:8055,136.186.108.120:8056"
	 * @throws SQLException 
	 */
	public static String getServerListWithoutId() throws SQLException {		
		Connection conn = DataBase.DBConnection();
        String serverList = null;
		// create a Statement
        try (Statement stmt = conn.createStatement()) {
            //execute query
        	String sqlString = "select server from Servers where status=1 order by serverid asc";
        	ResultSet rs = stmt.executeQuery(sqlString);
        	while (rs.next())
            {             
        		String serverAddress = rs.getString("server");
        		if(serverList==null) {
        			serverList = serverAddress;
        		}
        		else {
        			serverList = serverList + "," + serverAddress;
        		}
            }
        	stmt.close();
        }
		conn.close();		
		return serverList;
	}
	
	/**
	 * 	Given an ID, return the server's IP and Port
	 * @return one server with ID
	 * @throws SQLException 
	 */
	public static String getCurrentServerById(int id) throws SQLException {				
		Connection conn = DataBase.DBConnection();
        String server = null;
		// create a Statement
        try (Statement stmt = conn.createStatement()) {
            //execute query
        	String sqlString = "select server, serverid from Servers where status=1 and serverid =" + id;
        	ResultSet rs = stmt.executeQuery(sqlString);
        	if (rs.next())
            {             
        		server = rs.getString("server")+":"+rs.getInt("serverid");
            }
        	stmt.close();
        }
		conn.close();		
		return server;		
	}
	

}
