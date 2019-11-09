#!/bin/sh

NODE_NAME=$HOSTNAME
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
PARAFLOW_CONFIG_FILE="$PARAFLOW_HOME/config/collector.conf"

# modify the configuration file
modify_config()
{
  if [ -w $PARAFLOW_CONFIG_FILE ]; then
    sed -i '2s/.*/collector.id=collector-'$NODE_NAME'/' $PARAFLOW_CONFIG_FILE
  else
    echo "no write permission on collector.conf file"
    exit 0
  fi
}

modify_config
