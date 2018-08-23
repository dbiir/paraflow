#!/bin/bash

PIDS=$(ps ax | grep -i 'ParaflowCollector' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No paraflow collector to stop"
  exit 1
else
  kill -s TERM ${PIDS}
fi
