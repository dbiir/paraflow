#!/bin/sh
# start-paraflow&metaServer&presto
PREFIX="dbiir"
master_num=1
START=2
END=9
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_HEAP_OPTS="-Xmx16G -Xms8G"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
PARAFLOW_SBIN="/home/iir/opt/paraflow/sbin"
PARAFLOW_BIN="/home/iir/opt/paraflow/bin"
PRESTO_SBIN="/home/iir/opt/presto-server-0.192/sbin"
PRESTO_BIN="/home/iir/opt/presto-server-0.192/bin"

# start the Zookeeper
ssh $PREFIX"0"$master_num "chown -R iir:iir $PARAFLOW_SBIN"

ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/zookeeper-server.sh start"
echo "start zookeeper02-06 cluster from dbiir0$master_num"

# start the Kafka
ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/kafka-server.sh start"
echo "start kafka02-06 cluster from dbiir0$master_num"

#start the paraflow metaserver
ssh $PREFIX"0"$master_num "$PARAFLOW_BIN/paraflow-metaserver-start.sh &"
echo " metaserver on dbiir0$master_num have started in daemon"

#start the paraflow collector
#ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/paraflow-collector.sh start"
#echo "paraflow collector on dbiir0$master_num have started"
#echo "test-tpch topic have been created in kafka, creating data"

#start the paraflow loader
#ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/paraflow-loader.sh start"
#echo "paraflow loader on dbiir0$master_num have started"
#echo "loader is drawing data as a consumer from topic, at the same time, meta data sinking,relation data flushing to hdfs"


#start the presto server cluster01-09
ssh $PREFIX"0"$master_num "$PRESTO_SBIN/start-all.sh"
echo "presto server cluster on dbiir0$master_num - dbiir0$END have started"
echo "you can try to excute query on $PREFIX0$master_num"
echo "example :./bin/presto --server localhost:8080 --catalog paraflow --schema test"

