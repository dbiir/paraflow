#！/bin/bash

#path for lib
LIB_DIR=../dist/paraflow-1.0-alpha1/lib
BIN_DIR=../dist/paraflow-1.0-alpha1/bin
PLUGIN_DIR=../dist/paraflow-1.0-alpha1/plugin
VERSION_TAR=paraflow-1.0-alpha1.tar.gz
VERSION=paraflow-1.0-alpha1


#Check if $DEPLOY_DIR exists in $SSH_IP.
#If not exists, create the $DEPLOY_DIR. 
dir_exist ()
{
cat >dir_exist.exp<<EOF
#!/usr/bin/expect
#Login $SSH_IP
spawn ssh $USER_NAME@$SSH_IP
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "$IP_PW\r";}
}
expect "*#"
send "mkdir -p $DEPLOY_DIR\r"
expect "*#"
send "exit\r"
expect eof
EOF
chmod 755 dir_exist.exp
./dir_exist.exp > /dev/null 
/bin/rm -rf dir_exist.exp
}


#function for scp tar file to each server specified in servers file
scp_tar ()
{
cat >scp_tar.exp<<EOF
#!/usr/bin/expect
#Login $SSH_IP
spawn scp $VERSION_TAR $USER_NAME@$SSH_IP:$DEPLOY_DIR
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "$IP_PW\r";}
}
expect eof
EOF
chmod 755 scp_tar.exp
./scp_tar.exp > /dev/null 
#Logout $SSH_IP
/bin/rm -rf scp_tar.exp
}

#Untar the tar file to specified dir in each server
untar ()
{
cat >untar.exp<<EOF
#!/usr/bin/expect
#Login $SSH_IP
spawn ssh $USER_NAME@$SSH_IP
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "$IP_PW\r";}
}
expect "*#"
send "cd $DEPLOY_DIR\r"
expect "*#"
send "tar -zxvf $VERSION_TAR\r"
expect "*#"
send "exit\r"
expect eof
EOF
chmod 755 untar.exp
./untar.exp > /dev/null 
/bin/rm -rf dir_exist.exp
}

################################################main##########################################################

if [ $# = 6 ]
then
  #arg[1]: path for deploying
  DEPLOY_DIR=$1
  #arg[2]: path for server
  SERVER_DIR=$2
  #arg[3]: path for RealTimeAnalysis project
  REAL_DIR=$3
  #arg[4]: path for Presto project
  PRESTO_DIR=$4
  #arg[5]: Username
  USER_NAME=$5
  #arg[6]: Username
  IP_PW=$6
else 
  echo "Deployment Tool Usage"
  echo "./d.sh <Deployment Directory> <Server Directory> <RealTimeAnalysis> <Presto> <Username> <Password>"
  echo "Deploy Directory: path for deploying"
  echo "Server Directory: path for server"
  echo "RealTimeAnalysis: path for RealTimeAnalysis project"
  echo "Presto: path for Presto project"
  echo "Username for all nodes in the cluster"
  echo "Password for all nodes in the cluster"
  exit 0
fi

#delete the last '/' in dictionary if have
if [[ $DEPLOY_DIR == */ ]]
then
  DEPLOY_DIR=${DEPLOY_DIR%/*}
else
  :
fi
#delete dir "/home/presto
#delete the last '/' in dictionary if have
if [[ $SERVER_DIR == */ ]]
then
  SERVER_DIR=${SERVER_DIR%/*}
else
  :
fi
#delete the last '/' in dictionary if have
if [[ $REAL_DIR == */ ]]
then
  REAL_DIR=${REAL_DIR%/*}
else
  :
fi
#delete the last '/' in dictionary if have
if [[ $PRESTO_DIR == */ ]]
then
  PRESTO_DIR=${PRESTO_DIR%/*}
else
  :
fi

##Judge specified Deploying path is or isn't a valid dir
#if [ -d $DEPLOY_DIR ]
#then
#  :
#else
#  echo "Specified Deploying path is not a valid dir"
#  mkdir $DEPLOY_DIR || echo "mkdir deploy dir failure!" && exit 0
#fi

#Judge specified server path is or isn't a valid dir
if [ -d $SERVER_DIR ]
then
  :
else
  echo "Specified server path is not valid"
  exit 0
fi

if [ -d $REAL_DIR ]
then
  :
else
  echo "Specified RealTimeAnalysis path is not valid"
  exit 0
fi

if [ -d $PRESTO_DIR ]
then
  :
else
  echo "Specified Presto path is not valid"
  exit 0
fi

if [ -f $SERVER_DIR/servers ]
then
  :
else
  echo
  echo "-------------Please touch File: servers-------------"
  echo "############# servers For Example #############"
  echo "192.168.136.3"
  exit 0
fi

if ( rpm -qa | grep -q expect )
then 
  :
else
  yum -y install expect > /dev/null
fi


#Check if lib dir exists under the directory of ParaFlow/dist/ParaFlow-xxx/.
#If not exists, create the lib dir.
if [ -d $LIB_DIR ]
then
  : rm -f $LIB_DIR/*
else
  mkdir $LIB_DIR
fi

if [ -d $BIN_DIR ]
then
  :
else
  mkdir $BIN_DIR
fi

if [ -d $PLUGIN_DIR ]
then
  : rm -rf $PLUGIN_DIR/*
else
  mkdir $PLUGIN_DIR
fi

#Copy jar files from RealTimeAnalysis/dist/bin/ to lib
cp $REAL_DIR/dist/bin/*.jar $LIB_DIR/
#Copy jar files from Presto/presto-server/target/lib to lib
cp $PRESTO_DIR/presto-server/target/presto-server-0.158-SNAPSHOT/lib/*.jar $LIB_DIR/
cp -r $PRESTO_DIR/presto-server/target/presto-server-0.158-SNAPSHOT/bin/* $BIN_DIR/
cp -r $PRESTO_DIR/presto-server/target/presto-server-0.158-SNAPSHOT/plugin/hdfs $PLUGIN_DIR/
#Package the ParaFlow-xxx/ dir to a tar file as ParaFlow-xxx.tar
cd ../dist/
tar -zcvf $VERSION_TAR $VERSION 

#Loop for every server in servers to deploy
while read SSH_IP; do
  echo "-------------------$SSH_IP--------------------------"
  dir_exist
  if [ $? -eq 0 ]
  then
    echo "-------------$SSH_IP dir_exist is OK-----------"
  else
    echo "------------$SSH_IP dir_exist is failed-------------"
  fi
  scp_tar
  if [ $? -eq 0 ]
  then
    echo "-------------$SSH_IP scp_tar is OK-----------"
  else
    echo "------------$SSH_IP scp_tar is failed-------------"
  fi
  untar
  if [ $? -eq 0 ]
  then
    echo "-------------$SSH_IP untar is OK-----------"
  else
    echo "------------$SSH_IP untar is failed-------------"
  fi
done <$SERVER_DIR/servers
