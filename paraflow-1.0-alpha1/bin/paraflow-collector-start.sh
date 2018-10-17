#!/bin/bash

if [ $# -lt 9 ];
then
  echo "USAGE: $0 [-daemon] db_name table_name parallelism partition_num sf part part_count min_custkey max_custkey"
  exit 1
fi
base_dir=$(dirname $0)

MAIN_CLASS="cn.edu.ruc.iir.paraflow.examples.collector.BasicCollector"

if [ "xPARAFLOW_HOME" = "x" ]; then
  export PARAFLOW_HOME="$base_dir/../"
fi
if [ "x$PARAFLOW_LOG4J_OPTS" = "x" ]; then
  export PARAFLOW_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

if [ "x$PARAFLOW_HEAP_OPTS" = "x" ]; then
  export PARAFLOW_HEAP_OPTS="-Xmx4G -Xms1G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name ParaflowCollector'}

COMMAND=$1
case ${COMMAND} in
  -daemon)
    EXTRA_ARGS="-daemon "${EXTRA_ARGS}
    shift
    ;;
  *)
    ;;
esac

exec ${base_dir}/paraflow-run-class.sh ${EXTRA_ARGS} ${MAIN_CLASS} "$@"
