#!/bin/bash

PIDS=$(ps ax | grep -i 'ParaflowMetaServer' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No paraflow metaserver to stop"
  exit 1
else
  kill -s TERM $PIDS
fi
