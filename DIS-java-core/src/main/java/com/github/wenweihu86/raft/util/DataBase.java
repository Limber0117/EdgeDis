package com.github.wenweihu86.raft.util;

import java.sql.*;
import java.lang.Math;
import  com.github.wenweihu86.raft.util.EVENT;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;  

public class DataBase {

	public static void main(String [] args) throws SQLException, ClassNotFoundException{
		
		DatabaseLog dbLog = new DatabaseLog();
		dbLog.sid = (int)(10000*Math.random());
		dbLog.sip = "12.12.54.134";
		dbLog.event = EVENT.APPEND_DATA;
		dbLog.t_time = (float)(100*Math.random());
		dbLog.leader = 123;
		dbLog.term =dbLog.sid + 5;
		
		addToDB(dbLog);
	}
	
	public static Connection DBConnection() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:mysql://136.186.108.68:3306/Raft", "newroot", "newroot");
		return conn;
	}
	
	
	public static void addToDB(DatabaseLog dblog) throws SQLException, ClassNotFoundException{
		
		Connection conn = DBConnection();
        // create a Statement
        try (Statement stmt = conn.createStatement()) {
            //execute query
        	String sqlString = "insert into `Results` (sid, sip,event,start_time,end_time, t_time,entry, term, log_index, leader,detail) values ('"+ dblog.sid +"','" + dblog.sip +"'," 
            + dblog.eventToInt(dblog.event) + ",'" + dblog.start_time + "','" + dblog.end_time + "'," + dblog.t_time +"," + dblog.data_length + ",'" + dblog.term + "','" 
            + dblog.log_index + "','" + dblog.leader+"','" + dblog.detail + "')";
        	//System.out.println("the SQLStr is: \t" + sqlString);
           stmt.executeUpdate(sqlString);
           stmt.close();
           conn.close();
        }
        
		
	}
	
	public static void addDBBreak(String descriptionString) throws SQLException, ClassNotFoundException{
		
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://136.186.108.68:3306/Raft", "newroot", "newroot")) {
            // create a Statement
            try (Statement stmt = conn.createStatement()) {
                //execute query
            	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            	LocalDateTime now = LocalDateTime.now();
            	String sqlString = "insert into `Results` (sid, sip,event,start_time,end_time, t_time,entry, term, log_index, leader,detail) values ('"+ 0 +"','" + 0 +"'," 
                + 0 + ",'" + now + "','" +0 + "'," + 0 +"," + 0 + ",'" + 0 + "','" 
                + 0 + "','" + 0+"','" + descriptionString + "')";
            	//System.out.println("the SQLStr is: \t" + sqlString);
               stmt.executeUpdate(sqlString);
               stmt.close();
               conn.close();
            }
        }
		
	}
	
	
	
	
}
