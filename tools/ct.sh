#！/bin/bash


#定义function compilation
compilationRP ()
{
export MAVEN_OPTS=-Xmx512m
cd $PRESTO_DIR
mvn clean install -DskipTests
cd $REAL_DIR
mvn clean package -DskipTests
}


#定义function testRP
testRP ()
{
export MAVEN_OPTS=-Xmx512m
mvn test-compile
mvn test
}

#########################################################################main################################################################################


if [ $# = 3 ]
then
  #指定RealtimeAnalysis的目录
  REAL_DIR=$1
  #指定Presto的目录
  PRESTO_DIR=$2
  #用户选择操作代码
  OPT=$3
else 
  echo "参数不够!"
  echo "请按照RealtimeAnalysis目录，Presto目录，可选操作代码顺序输入三个参数"
  echo "可选代码含义如下："
  echo "1：只编译"
  echo "2：只测试"
  echo "3：既编译又测试"
  exit 0
fi


if [ -d $REAL_DIR ]
then
  :
else
  echo "目录格式输入有误or目录不存在"
  echo "第一个参数为RealtimeAnalysis的路径"
  exit 0
fi


echo "RealtimeAnalysis directory is $REAL_DIR"
echo "Presto directory is $PRESTO_DIR"
echo "Option number is $OPT"

if [ -d $PRESTO_DIR ]
then
  :
else
  echo "目录格式输入有误or目录不存在"
  echo "第二个参数为Presto的路径"
  exit 0
fi


if [ $OPT -eq 1 ]
then
  compilationRP
  if [ $? -eq 0 ]
  then
    echo "-------------Compilation is OK-----------"
  else
    echo "------------Compilation is failed-------------"
    exit 0
  fi
elif [ $OPT -eq 2 ]
then
  testRP
  if [ $? -eq 0 ]
  then
    echo "-------------Test is OK-----------"
  else
    echo "------------Test is failed-------------"
    exit 0
  fi
elif [ $OPT -eq 3 ]
then
  compilationRP
  if [ $? -eq 0 ]
  then
    echo "-------------Compilation is OK-----------"
  else
    echo "------------Compilation is failed-------------"
    exit 0
  fi
  testRP
  if [ $? -eq 0 ]
  then
    echo "-------------Test is OK-----------"
  else
    echo "------------Test is failed-------------"
    exit 0
  fi
else
  echo "可选操作代码输入不符合要求!"
  echo "1：只编译"
  echo "2：只测试"
  echo "3：既编译又测试"
  exit 0
fi
