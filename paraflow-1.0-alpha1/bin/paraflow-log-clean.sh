#!/bin/bash

base_dir=$(dirname $0)

if [ "x$PARAFLOW_HOME" = "x" ]; then
  export PARAFLOW_HOME="$base_dir/../"
fi
if [ "x$PARAFLOW_LOG4J_OPTS" = "x" ]; then
  export PARAFLOW_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

if [ "x$PARAFLOW_HEAP_OPTS" = "x" ]; then
  export PARAFLOW_HEAP_OPTS="-Xmx1G -Xms512M"
fi

java -jar $base_dir/paraflow-client-1.0-alpha1-allinone.jar clear 5
