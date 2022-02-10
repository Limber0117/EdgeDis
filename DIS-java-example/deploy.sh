#!/usr/bin/env bash

cd ../DIS-java-core && mvn clean install -DskipTests
cd -
mvn clean package

EXAMPLE_TAR=DIS-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p $ROOT_DIR
cd $ROOT_DIR

serverID = 1

for((folderNum=$serverID;folderNum<=128;folderNum = folderNum+23)
do
	mkdir node$folderNum
	cd node$folderNum
	cp -f ../../target/$EXAMPLE_TAR .
	tar -zxvf $EXAMPLE_TAR
	chmod +x ./bin/*.sh
	cd -
done

mkdir client
cd client
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
cd -
