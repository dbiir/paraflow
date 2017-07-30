#！/bin/bash

#path for lib
LIB_DIR=../dist/ParaFlow-1.0-alpha1/lib
#function for scp tar file to each server specified in servers file
scp_tar ()
{
#Login $SSH_IP
ssh $USER_NAME@$SSH_IP
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "$IP_PW\r";}
}
expect eof
#Check if $DEPLOY_DIR exists in $SSH_IP.
#If not exists, create the $DEPLOY_DIR.
if [ -d $DEPLOY_DIR ]
then
  :
else
  mkdir $DEPLOY_DIR || echo "mkdir on $SSH_IP failure!" && exit 0 
fi
#Logout $SSH_IP
exit
scp ParaFlow-1.0-alpha1.tar.gz $USER_NAME@$SSH_IP:$DEPLOY_DIR 
}


#Untar the tar file to specified dir in each server
untar ()
{
#Login $SSH_IP
ssh $USER_NAME@$SSH_IP
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "$IP_PW\r";}
}
expect eof
cd $DEPLOY_DIR
tar -zxvf ParaFlow-1.0-alpha1.tar.gz
exit
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
  #arg[6]: IP_PW
  IP_PW=$6
else 
  echo "D Tool Usage"
  echo "./d.sh <Deploy Dictionary> <server Dictionary>"
  echo "Deploy Dictionary: path for deploying"
  echo "Server Dictionary: path for server"
  echo "RealTimeAnalysis: path for RealTimeAnalysis project"
  echo "Presto: path for Presto project"
  echo "Username for every node"
  echo "Password for every node"
  exit 0
fi


if [ -d $DEPLOY_DIR ]
then
  :
else
  echo "Specified Deploying path is not a valid dir"
  exit 0
fi


if [ -d $SERVER_DIR ]
then
  :
else
  echo "Specified server path is not a valid dir"
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
  :
else
  mkdir ../dist/ParaFlow-1.0-alpha1/lib
fi

#Copy jar files from RealTimeAnalysis/dist/bin/ to lib
cp $REAL_DIR/dist/bin/*.jar $LIB_DIR/
#Copy jar files from Presto/presto-server/target/lib to lib
cp $PRESTO_DIR/presto-server/target/lib/*.jar $LIB_DIR/
#Package the ParaFlow-xxx/ dir to a tar file as ParaFlow-xxx.tar
cd ../dist/
tar -zcvf ParaFlow-1.0-alpha1.tar.gz ParaFlow-1.0-alpha1


while read SSH_IP; do
  echo $SSH_IP
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
done <servers

