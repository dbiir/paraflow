#!/bin/sh

echo $1

prefix="dbiir"
start=2
end=9

for ((i=$start; i <=$end; i++))
do
  if [ $i -lt 10 ]
  then
    ssh $prefix"0"$i $1
  else
    ssh $prefix$i $1
  fi
done

