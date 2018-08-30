#!/bin/sh
# init|start|stop|clean|destroy paraflow collectors

echo $1" paraflow collectors."

PREFIX="dbiir"
START=1
END=9
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_HEAP_OPTS="-Xmx2G -Xms2G"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
DB="test"
TABLE="tpch"
PARALLELISM="8"
PARTITION_NUM="320"
SF="300"
PART_COUNT="8"
MIN_CUSTKEY="0"
MAX_CUSTKEY="10000000"

# deployment
deploy()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "deploy paraflow collector on dbiir0$i"
      scp -r $PARAFLOW_DIR $PREFIX"0"$i:$PARAFLOW_DIR
      ssh $PREFIX"0"$i "$PARAFLOW_DIR/sbin/paraflow-init.sh"
    else
      echo "deploy paraflow collector on dbiir$i"
      scp -r $PARAFLOW_DIR $PREFIX$i:$PARAFLOW_DIR
      ssh $PREFIX$i "$PARAFLOW_DIR/sbin/paraflow-init.sh"
    fi
  done
}

# start
startup()
{
  part_index=1
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "starting paraflow collector on dbiir0"$i
      ssh $PREFIX"0"$i "export PARAFLOW_HEAP_OPTS=$PARAFLOW_HEAP_OPTS && $PARAFLOW_HOME/bin/paraflow-collector-start.sh $DB $TABLE $PARALLELISM $PARTITION_NUM $SF $partIndex $PART_COUNT $MIN_CUSTKEY $MAX_CUSTKEY"
    else
      echo "starting paraflow collector on dbiir"$i
      ssh $PREFIX$i "export PARAFLOW_HEAP_OPTS=$PARAFLOW_HEAP_OPTS && $PARAFLOW_HOME/bin/paraflow-collector-start.sh $DB $TABLE $PARALLELISM $PARTITION_NUM $SF $partIndex $PART_COUNT $MIN_CUSTKEY $MAX_CUSTKEY"
    fi
    ((part_index++))
  done
}

# stop
stop()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "stopping paraflow collector on dbiir0"$i
      ssh $PREFIX"0"$i "$PARAFLOW_HOME/bin/paraflow-collector-stop.sh"
    else
      echo "stopping paraflow collector on dbiir"$i
      ssh $PREFIX$i "$PARAFLOW_HOME/bin/paraflow-collector-stop.sh"
    fi
  done
}

# clean
cleanup()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "cleaning paraflow collector on dbiir0"$i
      ssh $PREFIX"0"$i "rm -rf $PARAFLOW_HOME/logs"
    else
      echo "cleaning paraflow collector on dbiir"$i
      ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/logs"
    fi
  done
}

# destroy
destroy()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "destroy paraflow collector on dbiir0"$i
      ssh $PREFIX"0"$i "rm -rf $PARAFLOW_HOME/ && rm -rf $PARAFLOW_HOME && rm -rf $PARAFLOW_DIR"
    else
      echo "destroy paraflow collector on dbiir"$i
      ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/ && rm -rf $PARAFLOW_HOME && rm -rf $PARAFLOW_DIR"
    fi
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
