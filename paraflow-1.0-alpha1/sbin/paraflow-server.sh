#!/bin/sh
# init|start|stop|clean|destroy the kafka cluster

echo $1" the paraflow loaders."

PREFIX="dbiir"
START=11
END=26
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_HEAP_OPTS="-Xmx4G -Xms2G"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
DB="test"
TABLE="tpch"
PARTITION_FROM="0"
PARTITION_TO="79"

# deployment
deploy()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "deploy paraflow on dbiir$i"
    scp -r $PARAFLOW_DIR $PREFIX$i:$PARAFLOW_DIR
    ssh $PREFIX$i "$PARAFLOW_DIR/sbin/paraflow-init.sh"
  done
}

# start
startup()
{
  range=$(($PARTITION_TO - $PARTITION_FROM + 1))
  part_range=$(($range / ($END - $START + 1)))
  for ((i=$START; i<=$END; i++))
  do
    part_id=$(($i - $START))
    part_from=$(($part_id * $part_range))
    part_to=$(($part_id * $part_range + $part_range - 1))
    echo "starting paraflow loader on dbiir"$i" from $part_from to $part_to."
    ssh $PREFIX$i "export PARAFLOW_HEAP_OPTS=$PARAFLOW_HEAP_OPTS && $PARAFLOW_HOME/bin/paraflow-loader-start.sh $DB $TABLE $part_from $part_to"
  done
}

# stop
stop()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "stopping paraflow loader on dbiir"$i
    ssh $PREFIX$i "$PARAFLOW_HOME/bin/paraflow-server-stop.sh"
  done
}

# clean
cleanup()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "cleaning paraflow loader on dbiir"$i
    ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/logs"
  done
}

# destroy
destroy()
{
  for ((i=$START; i<=$END; i++))
  do
    echo "destroy paraflow on dbiir0"$i
    ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/ && rm -rf $PARAFLOW_HOME && rm -rf $PARAFLOW_DIR"
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
