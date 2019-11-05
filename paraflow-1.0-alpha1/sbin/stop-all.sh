!/bin/sh
# stop-kafka-paraflow&metaServer&presto
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

#stop the paraflow loaders
ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/paraflow-loader.sh stop"
echo "paraflow loader on dbiir0$master_num stopped"
echo "loader is drawing data as a consumer from topic, at the same time, meta data sinking,relation data flushing to hdfs"

#stop the paraflow collector
ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/paraflow-collector.sh stop"
echo "paraflow collector on dbiir0$master_num have stopped"

# stop the Zookeeper
ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/zookeeper-server.sh stop"
echo " zookeeper02-06 cluster from dbiir0$master_num stopped"

# stop the Kafka
ssh $PREFIX"0"$master_num "$PARAFLOW_SBIN/kafka-server.sh stop"
echo "kafka02-06 cluster from dbiir0$master_num stop"

#stop the paraflow metaserver in daemon use &
ssh $PREFIX"0"$master_num "$PARAFLOW_BIN/paraflow-metaserver-stop.sh &"
echo " metaserver on dbiir0$master_num stopped in daemon"

#stop the presto server cluster01-09
ssh $PREFIX"0"$master_num "$PRESTO_SBIN/stop-all.sh"
echo "presto server cluster on dbiir0$master_num - dbiir0$END stopped"

echo "paraflow and its depencies entirely stop"
