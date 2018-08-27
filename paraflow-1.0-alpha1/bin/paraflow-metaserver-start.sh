#!/bin/bash

base_dir=$(dirname $0)/..

if [ "x$PARAFLOW_HOME" = "x" ]; then
  export PARAFLOW_HOME="$base_dir/"
fi
if [ "x$PARAFLOW_LOG4J_OPTS" = "x" ]; then
  export PARAFLOW_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

if [ "x$PARAFLOW_HEAP_OPTS" = "x" ]; then
  export PARAFLOW_HEAP_OPTS="-Xmx1G -Xms512M"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name ParaflowMetaServer'}

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac

exec $base_dir/paraflow-run-class.sh $EXTRA_ARGS cn.edu.ruc.iir.paraflow.metaserver.server.MetaServer
