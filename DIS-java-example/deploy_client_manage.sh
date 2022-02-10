#!/usr/bin/env bash

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


mkdir manage
cd manage
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR

ADMIN_TAR=DIS-java-admin-1.9.0.jar
cd bin
cp -f ../../../../DIS-java-admin/target/$ADMIN_TAR .
cd -
chmod +x ./bin/*.sh
cd -
