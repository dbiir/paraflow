#!/bin/sh

data_path="/data1/zookeeper"
ZK_DIR="/home/iir/opt/zookeeper-3.4.13"
ZK_HOME="/home/iir/opt/zookeeper"
ZK_CONFIG_FILE="$ZK_HOME/conf/zoo.cfg"
NODE_NAME=$HOSTNAME
NODE_ID=${NODE_NAME:$((${#NODE_NAME}-1)):1}
ZK_NODE_ID=$(($NODE_ID - 2))

create_dirs()
{
  if [ $(id -u) != "0" ]; then
    echo "please run $0 in root."
    exit 0
  fi
  cd $ZK_DIR/.. && ln -s $ZK_DIR zookeeper && chown -h iir:iir zookeeper
  mkdir $data_path && chown iir:iir $data_path
}

modify_config()
{
  if [ -w $ZK_CONFIG_FILE ]; then
    sed -i '14s/.*/clientPort='$((2181 + $ZK_NODE_ID))'/' $ZK_CONFIG_FILE
  else
    echo "no write permission on zoo.cfg file"
    exit 0
  fi
}

create_dirs
modify_config
