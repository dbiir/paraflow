#!/bin/bash
cd /home/iir/opt/paraflow/logs;ls | egrep -v "*.out" | xargs rm -rf
