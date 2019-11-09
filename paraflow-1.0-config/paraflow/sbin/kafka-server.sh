#!/bin/sh
# init|start|stop|clean|destroy the kafka cluster

echo $1" the kafka cluster."

PREFIX="dbiir"
START=2
END=9
KAFKA_HOME="/home/iir/opt/kafka"
KAFKA_HEAP_OPTS="-Xmx8G -Xms2G"
KAFKA_DIR="/home/iir/opt/kafka_2.11-1.1.1"

# deployment
deploy()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 $1 in root."
    exit 0
  fi
  for ((i=$START; i<=$END; i++))
  do
    echo "deploy kafka on dbiir0"$i
    scp -r $KAFKA_DIR $PREFIX"0"$i:$KAFKA_DIR
    ssh $PREFIX"0"$i "chown iir:iir $KAFKA_DIR"
    ssh $PREFIX"0"$i "$KAFKA_DIR/sbin/kafka-init.sh"
  done
}

# start
startup()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "starting kafka on dbiir0"$i
    ssh $PREFIX"0"$i "export KAFKA_HEAP_OPTS='$KAFKA_HEAP_OPTS' && $KAFKA_HOME/bin/kafka-server-start.sh -daemon /home/iir/opt/kafka/config/server.properties"
  done
}

# stop
stop()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "stopping kafka on dbiir0"$i
    ssh $PREFIX"0"$i "$KAFKA_HOME/bin/kafka-server-stop.sh"
  done
}

# clean
cleanup()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "cleaning kafka on dbiir0"$i
    ssh $PREFIX"0"$i "rm -rf $KAFKA_HOME/logs && rm -rf /data1/kafka/logs && rm -rf /data2/kafka/logs"
  done
}

# destroy
destroy()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "destroy kafka on dbiir0"$i
    ssh $PREFIX"0"$i "rm -rf $KAFKA_HOME/ && rm -rf $KAFKA_HOME && rm -rf $KAFKA_DIR && rm -rf /data1/kafka && rm -rf /data2/kafka"
  done
}

if [ "$1" == "deploy" ]; then
  deploy
elif [ "$1" == "start" ]; then
  startup
elif  [ "$1" == "stop" ]; then
  stop
elif [ "$1" == "clean" ]; then
  cleanup
elif [ "$1" == "destroy" ]; then
  destroy
# nothing
else
  echo "Usage: $0 deploy|start|stop|clean|destroy"
fi
