#!/bin/bash

#path
ZOOKEEPER_HOME=/usr/local/zookeeper/bin
HADOOP_HOME=/usr/local/hadoop/sbin
KAFKA_HOME=/usr/local/kafka/bin

################################################

cd $ZOOKEEPER_HOME
./zkServer.sh start
cd $HADOOP_HOME
./start-all.sh
cd $KAFKA_HOME
./kafka-server-start.sh ../config/server.properties
