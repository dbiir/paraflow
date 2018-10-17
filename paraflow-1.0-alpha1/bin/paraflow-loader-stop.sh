#!/bin/bash

PIDS=$(ps ax | grep -i 'BasicLoader' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No paraflow loader to stop"
  exit 1
else
  kill -s TERM ${PIDS}
fi
