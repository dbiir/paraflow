#!/bin/bash

for ((i=2; i<=9; i++))
do
echo "*****now is dbiir$i"
  #scp /home/iir/opt/pixels/pixels-daemon-0.1.0-SNAPSHOT-full.jar iir@dbiir0$i:/home/iir/opt/pixels/
  scp /home/iir/opt/paraflow/config/paraflow.properties  iir@dbiir0$i:/home/iir/opt/presto-server-0.192/etc/catalog/
  #scp /home/iir/opt/presto-server-0.192/etc/event-listener.properties iir@dbiir0$i:/home/iir/opt/presto-server-0.192/etc/

done

