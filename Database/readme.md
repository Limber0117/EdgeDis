# Introduction

Please note that the following parameter kept in database are only used to facilitate the experiment. They are not needed and can be easily removed when deploy the system in real-world practice.

# Results table for experiment results collection
```
  create table Results(
    id int(32) UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    sid varchar(100) not null, 
    sip varchar(100) not null, 
    event int not null,
    start_time varchar(100), 
    end_time varchar(100), 
    t_time float(10,2), 
    entry int(32), 
    term varchar(50), 
    log_index varchar(50),
    leader varchar(50), 
    detail varchar(100),
    updateTime DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
  );
```

## Servers table for keeping edge server IPs and Ports
```
  create table Servers (
    id int(32) UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
	  server varchar(100) not null, // server address (ip:port)
	  serverid int not null, // ID of current node
	  status int not null default 0, // 0 down and 1 up
	  detail varchar(100);// used to store some details that cannot stored to other columns.
  );
```
### initialized values
Please revise the IP and Port values according the experiment settings. The server id is equal to the id set for each edge server using the run_server.sh when setting up the system. The last parameter is 1 when the corresponding edge server is in use, and is 0 otherwise.

```
  insert into Servers(server,serverid,status) values ("136.186.108.234:8001",1,1);
  insert into Servers(server,serverid,status) values ("136.186.108.186:8002",2,1);
  insert into Servers(server,serverid,status) values ("136.186.108.236:8003",3,1);
  insert into Servers(server,serverid,status) values ("136.186.108.247:8004",4,1);
  insert into Servers(server,serverid,status) values ("136.186.108.93:8005",5,1);
  insert into Servers(server,serverid,status) values ("136.186.108.130:8006",6,1);
  insert into Servers(server,serverid,status) values ("136.186.108.20:8007",7,1);
  insert into Servers(server,serverid,status) values ("136.186.108.182:8008",8,1);
  insert into Servers(server,serverid,status) values ("136.186.108.27:8009",9,0);
  insert into Servers(server,serverid,status) values ("136.186.108.26:8010",10,0);
  insert into Servers(server,serverid,status) values ("136.186.108.240:8011",11,0);
  insert into Servers(server,serverid,status) values ("136.186.108.179:8012",12,0);
  insert into Servers(server,serverid,status) values ("136.186.108.59:8013",13,0);
  insert into Servers(server,serverid,status) values ("136.186.108.128:8014",14,0);
  insert into Servers(server,serverid,status) values ("136.186.108.137:8015",15,0);
.....
```

## Params table for other parameters
- create table
```
  create table Params (
    id int(32) UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    paraName varchar(100) not null, // server address (ip:port)
	  paraValue int not null,//
	  detail varchar(100);// used to store some details that cannot stored to other columns.
  );
```
- insert initial values

```
  insert into Params(paraName,paraValue) values ("electionTimeoutMilliseconds",2500);
  insert into Params(paraName,paraValue) values ("heartbeatPeriodMilliseconds",1000);
  insert into Params(paraName,paraValue) values ("maxAwaitTimeout",800);
```

