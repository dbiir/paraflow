#!/bin/bash

echo $1
echo $2

for ((i=2; i<=9; i++))
do
echo ""
echo "*****now is dbiir$i"
  #scp /home/iir/opt/pixels/pixels-daemon-0.1.0-SNAPSHOT-full.jar iir@dbiir0$i:/home/iir/opt/pixels/
  scp -r $1  iir@dbiir0$i:$2/

done
echo "task is done."
