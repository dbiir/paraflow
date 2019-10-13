#!/bin/bash
PREFIX="dbiir"
START=2
END=9
PARAFLOW_HOME="/home/iir/opt/paraflow"
PARAFLOW_DIR="/home/iir/opt/paraflow-1.0-alpha1"

# deployment
deploy()
{
	if [ $(id -u) != "0" ]; then
		echo "please run $0 $1 in root."
    	exit 0
  	fi

	for ((i=$START; i<=$END; i++))
  	do
	  if [ $i -lt 10 ]; then
	  	echo "init the logclc.sh on dbiir0$i"
        scp $PARAFLOW_DIR"/sbin/paraflow_logclean.sh" $PREFIX"0"$i:$PARAFLOW_DIR"/sbin/"
     	ssh $PREFIX"0"$i "chown -R iir:iir $PARAFLOW_DIR"
    fi
    done
}

# start
startup()
{
  for ((i=$START; i<=$END; i++))
  do
    ssh $PREFIX"0"$i "$PARAFLOW_DIR/sbin/paraflow_logclean.sh"
	echo "0$i clean have finished,content: the log produced by paraflow" 
done
}	



if [ "$1" == "deploy" ]; then
  deploy
elif [ "$1" == "start" ]; then
  startup

# nothing
else
  echo "Usage: $0 deploy|start"
  echo "run function:  deploy & start"
  deploy
  startup
  echo "clear log ok"
fi