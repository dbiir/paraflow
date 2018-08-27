#!/usr/bin/env bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name serviceName] [-loggc] classname [opts]"
  exit 1
fi

# get base dir
base_dir=$(dirname $0)/..
echo $base_dir

# add local jars into classpath
for file in "$base_dir"/libs/*.jar;
do
  CLASSPATH="$CLASSPATH":"$file"
done

if [ "x$PARAFLOW_HOME" = "x" ]; then
  export PARAFLOW_HOME="$base_dir/"
fi

# JMX settings
if [ -z "PARAFLOW_JMX_OPTS" ]; then
  PARAFLOW_JMX_OPTS="-Dcom.sun.management.jxmremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port
if [ $JMX_PORT ]; then
  PARAFLOW_JMX_OPTS="$PARAFLOW_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "PARAFLOW_LOG4J_OPTS" ]; then
  LOG4J_PATH="$base_dir/config/log4j.properties"
  PARAFLOW_LOG4J_OPTS="-Dlog4j.configuration=file:{$LOG4J_PATH}"
fi

# Generic jvm settings
if [ -z "$PARAFLOW_OPTS" ]; then
  PARAFLOW_OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$PARAFLOW_HEAP_OPTS" ]; then
  PARAFLOW_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$PARAFLOW_JVM_PERFORMANCE_OPTS" ]; then
  PARAFLOW_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

# Parse commands
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$PARAFLOW_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  PARAFLOW_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
fi

PARAFLOW_OPTS="-Dname=$DAEMON_NAME"

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  echo "$DAEMON_NAME running in daemon mode."
  nohup ${JAVA} ${PARAFLOW_HEAP_OPTS} ${PARAFLOW_JVM_PERFORMANCE_OPTS} ${PARAFLOW_GC_LOG_OPTS} ${PARAFLOW_JMX_OPTS} ${PARAFLOW_LOG4J_OPTS} -cp ${CLASSPATH} ${PARAFLOW_OPTS} "$@" > ${CONSOLE_OUTPUT_FILE} 2>&1 < /dev/null &
else
  exec ${JAVA} ${PARAFLOW_HEAP_OPTS} ${PARAFLOW_JVM_PERFORMANCE_OPTS} ${PARAFLOW_GC_LOG_OPTS} ${PARAFLOW_JMX_OPTS} ${PARAFLOW_LOG4J_OPTS} -cp ${CLASSPATH} ${PARAFLOW_OPTS} "$@"
fi
