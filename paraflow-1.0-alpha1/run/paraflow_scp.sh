#!/bin/bash

host="dbiir01"
PRESTO_DIR=/home/iir/opt/presto-server-0.192/
scp -r /home/tao/software/station/DBIIR/paraflow/paraflow-connector/target/paraflow-connector-1.0-alpha1 iir@$host:/home/iir/opt/presto-server-0.192/plugin/

#ssh iir@$host "$PRESTO_DIR/sbin/mv-plugins.sh"

echo "task is done."

#/home/iir/opt/paraflow/run/exe_script.sh "tail -f /home/iir/opt/presto-server-0.192/data/var/log/server.log"

#/home/iir/opt/paraflow/run/exe_script.sh "tail -10 ~/opt/presto-server-0.192/data/var/log/server.log"