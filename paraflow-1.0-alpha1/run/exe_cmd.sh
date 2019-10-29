#!/bin/sh

if [ $# != 1 ]; then
    echo "1> "
    echo "2> "
    echo "3> "
    echo "4> "
    exit 1;
fi

if [ $1 -eq 1 ]; then
    /home/iir/opt/paraflow/run/exe_script.sh "tail -10 ~/opt/presto-server-0.192/data/var/log/server.log"
elif [ $1 -eq 2 ]; then
    /home/iir/opt/paraflow/run/exe_script.sh "tail -f ~/opt/presto-server-0.192/data/var/log/server.log"
else
    echo "Param is invalid."
fi
