#!/bin/sh
# init|start|stop|clean|destroy paraflow collectors

echo $1" paraflow loaders."

PREFIX="dbiir"
START=11
END=26
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_HEAP_OPTS="-Xmx16G -Xms8G"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
DB="test"
TABLE="line"
PARTITION_FROM=0
PARTITION_TO=319

# deployment
deploy()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 $1 in root."
    exit 0
  fi
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "deploy paraflow loader on dbiir0$i"
      scp -r $PARAFLOW_DIR $PREFIX"0"$i:$PARAFLOW_DIR
      ssh $PREFIX"0"$i "chown -R iir:iir $PARAFLOW_DIR"
      ssh $PREFIX"0"$i "$PARAFLOW_DIR/sbin/paraflow-init.sh"
    else
      echo "deploy paraflow loader on dbiir$i"
      scp -r $PARAFLOW_DIR $PREFIX$i:$PARAFLOW_DIR
      ssh $PREFIX$i "chown -R iir:iir $PARAFLOW_DIR"
      ssh $PREFIX$i "$PARAFLOW_DIR/sbin/paraflow-init.sh"
    fi
  done
}

# start
startup()
{
  range=$(($PARTITION_TO - $PARTITION_FROM + 1))
  node_num=$(($END - $START + 1))
  part_range=$(($range / $node_num))
  for ((i=$START; i<=$END; i++))
  do
    if [ $i = $END ]; then
      part_id=$(($i - $START))
      part_from=$(($part_id * $part_range))
      part_to=$PARTITION_TO
    else
      part_id=$(($i - $START))
      part_from=$(($part_id * $part_range))
      part_to=$(($part_id * $part_range + $part_range - 1))
    fi
    if [ $i -lt 10 ]; then
      echo "starting paraflow loader on dbiir0"$i
      ssh $PREFIX"0"$i "export PARAFLOW_HEAP_OPTS='$PARAFLOW_HEAP_OPTS' && $PARAFLOW_HOME/bin/paraflow-loader-start.sh -daemon $DB $TABLE $part_from $part_to"
    else
      echo "starting paraflow loader on dbiir"$i
      ssh $PREFIX$i "export PARAFLOW_HEAP_OPTS='$PARAFLOW_HEAP_OPTS' && $PARAFLOW_HOME/bin/paraflow-loader-start.sh -daemon $DB $TABLE $part_from $part_to"
    fi
  done
}

# stop
stop()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "stopping paraflow loader on dbiir0"$i
      ssh $PREFIX"0"$i "$PARAFLOW_HOME/bin/paraflow-loader-stop.sh"
    else
      echo "stopping paraflow loader on dbiir"$i
      ssh $PREFIX$i "$PARAFLOW_HOME/bin/paraflow-loader-stop.sh"
    fi
  done
}

# clean
cleanup()
{
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "cleaning paraflow loader on dbiir0"$i
      ssh $PREFIX"0"$i "rm -rf $PARAFLOW_HOME/logs"
    else
      echo "cleaning paraflow loader on dbiir"$i
      ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/logs"
    fi
  done
}

# destroy
destroy()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 $1 in root."
    exit 0
  fi
  for ((i=$START; i<=$END; i++))
  do
    if [ $i -lt 10 ]; then
      echo "destroy paraflow loader on dbiir0"$i
      ssh $PREFIX"0"$i "rm -rf $PARAFLOW_HOME/ && rm -rf $PARAFLOW_HOME && rm -rf $PARAFLOW_DIR"
    else
      echo "destroy paraflow loader on dbiir"$i
      ssh $PREFIX$i "rm -rf $PARAFLOW_HOME/ && rm -rf $PARAFLOW_HOME && rm -rf $PARAFLOW_DIR && rm -rf /dev/shm/paraflow"
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
