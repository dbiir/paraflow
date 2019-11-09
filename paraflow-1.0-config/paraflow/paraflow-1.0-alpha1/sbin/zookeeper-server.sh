#!/bin/sh
# init|start|stop|clean|destroy the kafka cluster

echo $1" the zookeeper cluster."

PREFIX="dbiir"
START=2
END=6
ZK_DIR="/home/iir/opt/zookeeper-3.4.13"
ZK_HOME="/home/iir/opt/zookeeper"

startup()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "starting zookeeper on dbiir0"$i
    ssh $PREFIX"0"$i "/home/iir/opt/zookeeper/bin/zkServer.sh start"
  done
}

stop()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "stopping zookeeper on dbiir0"$i
    ssh $PREFIX"0"$i "/home/iir/opt/zookeeper/bin/zkServer.sh stop"
  done
}

deploy()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 in root."
    exit 0
  fi
  for ((i=$START; i<=$END; i++))
  do
    echo "deploy zookeeper on dbiir0"$i
    scp -r $ZK_DIR $PREFIX"0"$i:$ZK_DIR
    ssh $PREFIX"0"$i "chown iir:iir $ZK_DIR"
    ssh $PREFIX"0"$i "$ZK_DIR/sbin/zookeeper-init.sh"
  done
}

cleanup()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "destroy kafka on dbiir0"$i
    ssh $PREFIX"0"$i "rm -rf /data1/zookeeper/logs"
  done
}

destroy()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "destroy kafka on dbiir0"$i
    ssh $PREFIX"0"$i "rm -rf $ZK_HOME/ && rm -rf $ZK_HOME && rm -rf $ZK_DIR && rm -rf /data1/zookeeper"
  done
}

if [ "$1" == "start" ]; then
  startup
elif  [ "$1" == "stop" ]; then
  stop
elif [ "$1" == "deploy" ]; then
  deploy
elif [ "$1" == "clean" ]; then
  cleanup
elif [ "$1" == "destroy" ]; then
  destroy
else
  echo "Usage: $0 deploy|start|stop|clean|destroy"
fi
