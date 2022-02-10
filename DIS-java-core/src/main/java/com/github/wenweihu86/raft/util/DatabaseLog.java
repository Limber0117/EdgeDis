package com.github.wenweihu86.raft.util;

import  com.github.wenweihu86.raft.util.EVENT;

public class DatabaseLog {
	
	public int sid; // server id of current edge server;
	public String sip; // ip address of current edge server node;
	
	public EVENT event;  // enum value that stands for the type of current operation.
	public String start_time;
	public String end_time;
	public float t_time;           // corresponds to t_time (running time) and unit is float(10,2)
	public long data_length;  // corresponds to entry int(32) in database;
	public long term;
	public long log_index;  // corresponds to the last_committed_log_index
	public int leader; // the leader's id;
	public String detail;  //used to store some details that cannot stored to other columns.
	
	
	public DatabaseLog() {
		
		
	}
	
	
	// this function change the enum to int values before inserting to database
	public int eventToInt(EVENT event) {
		int eventInteger = 0;
		switch(event) {
			case CONVERT_STATUS:
				eventInteger = 1;
				break;
			case BECOME_LEADER:
				eventInteger = 2;
				break;
			case APPEND_DATA:
				eventInteger = 3;
				break;
			case APPEND_NODE:
				eventInteger = 4;
				break;
			case REMOVE_NODE:
				eventInteger = 5;
				break;	
			case START_PRE_VOTE:
				eventInteger = 6;
				break;					
			case FINISH_PRE_VOTE:
				eventInteger = 7;
				break;	
			case START_FORMAL_VOTE:
				eventInteger = 8;
				break;	
			case FINISH_FORMAL_VOTE:
				eventInteger = 9;
				break;	
		}
		return eventInteger;
	}
	
	
}


