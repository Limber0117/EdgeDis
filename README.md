# Environment settings
- download the entire project, unzip the three zip files and put the corresponding subfolders and files into each of the three folders, i.e., DIS-java-example, DIS-java-core, and DIS-java-admin

- start up Eclipse IDE, import the project through "Maven", i.e., Existing Maven Projects

- import the project step by step accordingly.


# Build

 - with menu "Run/Run as...", choose "Marven Clean"
 - choose "Marven build"
 - choose "Marven install"

If everything is correct, you will obtain some .jar files as well as some .jar.gz files in each of the three folders.

# Deploy

 All the commands used in this step have been writen in .sh files, which can be found in the DIS-java-example folder in the project.

 ## copy the entire project folder onto different edge servers (can be either physical or VM servers)

 ## for each edge server
 - go to the DIS-java-example subfolder 

 - execute the following commands in terminal to deploy each edge server
```
cd ../DIS-java-core && mvn clean install -DskipTests
cd -
mvn clean package

EXAMPLE_TAR=DIS-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p $ROOT_DIR
cd $ROOT_DIR

	mkdir node1  (Note: here "node1" is the first edge server, it can be any folder name in practice)
	cd node1
	cp -f ../../target/$EXAMPLE_TAR .
	tar -zxvf $EXAMPLE_TAR
	chmod +x ./bin/*.sh
	cd -
  
```

 ## for the remote app vendor cloud
  - execute the following commands in terminal
  
```
cd ../DIS-java-core && mvn clean install -DskipTests
cd -
mvn clean package

EXAMPLE_TAR=DIS-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p $ROOT_DIR
cd $ROOT_DIR

mkdir client
cd client
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
cd -
```

 ## for the system management module
 
 This module can be deployed at any edge server or the app vendor cloud side. It needs to be deployed only once. It is used to initialize the system, setup essential parameters, stop/start the system, and so on.

  - execute the following commands in terminal
```
cd ../DIS-java-core && mvn clean install -DskipTests
cd -
mvn clean package

ADMIN_TAR=raft-java-admin-1.9.0.jar
cd bin
cp -f ../../../../raft-java-admin/target/$ADMIN_TAR .
cd -
chmod +x ./bin/*.sh
cd -
```

## database

To facilitate the experiments, some essential parameters are kept in MySQL database. However, this can be removed in practice.  Please refer to the Database folder to create the tables accordingly and set up the usename, password, address for accessing MySQL in the Java program (in the DIS-java-core/.../Util/Database.java DBConnection())

# Start up the system

## start up the edge servers, for each edge server
- go to the created root folder, e.g., node 1 on the first edge server
- run the following command
```
nohup ./bin/run_server.sh 1  (note: here 1 means the ID of current edge server, the first edge server's ID is 1, the second's is 2, and so on.)
```

## inspect the system status
- find out current coordinator
```
./bin/run_manage.sh getLeader
```
- get the list of all running edge server and their status
```
./bin/run_manage.sh getCluster
```

## transmit data from cloud to edge servers
 - go to client folder
 - run the following command
 ```
 run_client.sh 1000000 30  (use simulated data, the parameters mean the distributed data is 30MB with each data block is 1MB) 
 ```

