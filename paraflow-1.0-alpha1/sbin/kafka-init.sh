#!/bin/sh

data_path=( "/data1/kafka" "/data1/kafka/logs" "/data2/kafka" "/data2/kafka/logs" )
NODE_NAME=$HOSTNAME
NODE_ID=${NODE_NAME:$((${#NODE_NAME}-1)):1}
KAFKA_NODE_ID=$(($NODE_ID - 2))
KAFKA_HOME="/home/iir/opt/kafka"
KAFKA_DIR="/home/iir/opt/kafka_2.11-1.1.1"
KAFKA_CONFIG_FILE="$KAFKA_HOME/config/server.properties"

# create a symbolic file and data directories
create_dirs()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 in root."
    exit 0
  fi
  cd $KAFKA_DIR/.. && ln -s $KAFKA_DIR kafka && chown -h iir:iir kafka
  for path in "${data_path[@]}"; do
    if [ ! -e $path ]; then
      mkdir $path
      chown iir:iir $path
    fi
  done
}

# modify the configuration file
modify_config()
{
  if [ -w $KAFKA_CONFIG_FILE ]; then
    sed -i '21s/.*/broker.id='$KAFKA_NODE_ID'/' $KAFKA_CONFIG_FILE
    sed -i '31s/.*/listeners=PLAINTEXT:\/\/:'$((9092 + $KAFKA_NODE_ID))'/' $KAFKA_CONFIG_FILE
  else
    echo "no write permission on server.properties file"
    exit 0
  fi
}

create_dirs
modify_config
