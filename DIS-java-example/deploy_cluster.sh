#!/usr/bin/env bash
cd ../DIS-java-core && mvn clean install -DskipTests
cd -
mvn clean package

EXAMPLE_TAR=DIS-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p $ROOT_DIR
cd $ROOT_DIR


for F in {1..129..32}
do
	mkdir node${F}
	cd node${F}
	cp -f ../../target/$EXAMPLE_TAR .
	tar -zxvf $EXAMPLE_TAR
	chmod +x ./bin/*.sh
	cd -
done
