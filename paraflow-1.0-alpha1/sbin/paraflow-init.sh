#!/bin/sh

NODE_NAME=$HOSTNAME
NODE_ID=${NODE_NAME:$((${#NODE_NAME}-1)):1}
PARAFLOW_NODE_ID=$(($NODE_ID))
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"
PARAFLOW_CONFIG_FILE="$PARAFLOW_HOME/config/loader.conf"

# create a symbolic file and data directories
create_dirs()
{
  mkdir /dev/shm/paraflow && chown -R iir:iir /dev/shm/paraflow
  cd $PARAFLOW_DIR/.. && ln -s $PARAFLOW_DIR paraflow && chown -h iir:iir paraflow
}

# modify the configuration file
modify_config()
{
  if [ -w $PARAFLOW_CONFIG_FILE ]; then
    sed -i '2s/.*/loader.id='$PARAFLOW_NODE_ID'/' $PARAFLOW_CONFIG_FILE
  else
    echo "no write permission on loader.conf file"
    exit 0
  fi
}

create_dirs
modify_config
