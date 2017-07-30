#！/bin/bash

#function for compilation
compilationRP ()
{
cd $PRESTO_DIR
mvn clean install -DskipTests
cd $REAL_DIR
mvn clean package -DskipTests
}

#function for test
testRP ()
{
mvn test-compile
mvn test
}

#########################################################################main################################################################################

if [ $# = 3 ]
then
  #arg[1]: path for RealTimeAnalysis project
  REAL_DIR=$1
  #arg[2]: path for Presto project
  PRESTO_DIR=$2
  #arg[3]: running mode. 1 for compilation only, 2 for test only, 3 for compilation and test
  OPT=$3
else 
  echo "CT Tool Usage"
  echo "./ct.sh <RealTimeAnalysis> <Presto> <mode>"
  echo "RealTimeAnalysis: path for RealTimeAnalysis project"
  echo "Presto: path for Presto project"
  echo "mode：running mode. 1 for compilation, 2 for test, 3 for compilation and test"
  exit 0
fi

if [ -d $REAL_DIR ]
then
  :
else
  echo "Specified RealTimeAnalysis path is not a valid dir"
  exit 0
fi

if [ -d $PRESTO_DIR ]
then
  :
else
  echo "Specified Presto path is not a valid dir"
  exit 0
fi

echo "RealtimeAnalysis path is $REAL_DIR"
echo "Presto path is $PRESTO_DIR"
echo "Running mode is $OPT"

if [ $OPT -eq 1 ]
then
  compilationRP
  if [ $? -eq 0 ]
  then
    echo "-------------Compilation DONE-----------"
  else
    echo "------------Compilation FAILED-------------"
    exit 0
  fi
elif [ $OPT -eq 2 ]
then
  testRP
  if [ $? -eq 0 ]
  then
    echo "-------------Test DONE-----------"
  else
    echo "------------Test FAILED-------------"
    exit 0
  fi
elif [ $OPT -eq 3 ]
then
  compilationRP
  if [ $? -eq 0 ]
  then
    echo "-------------Compilation DONE-----------"
  else
    echo "------------Compilation FAILED-------------"
    exit 0
  fi
  testRP
  if [ $? -eq 0 ]
  then
    echo "-------------Test DONE-----------"
  else
    echo "------------Test FAILED-------------"
    exit 0
  fi
else
  echo "Specified running mode is not valid"
  echo "1：compilation only"
  echo "2：test only"
  echo "3：compilation and test"
  exit 0
fi
