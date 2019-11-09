# ParaFlow

ParaFlow is an interactive analysis system for OLAP developed at [DBIIR Lab @ RUC](http://iir.ruc.edu.cn).

#### Install & Deploy
##### Zookeeper-3.4.13
This is required by Kafka.
##### Kafka-2.11_1.11
##### Postgresql-9.5
##### Presto-0.192
##### Paraflow
1. MetaServer
2. Loader [cn.edu.ruc.iir.paraflow.example.loader.BasicLoader]
`./sbin/paraflow-loader.sh deploy`
3. Collector [cn.edu.ruc.iir.paraflow.example.loader.BasicCollector]
`./sbin/paraflow-collector.sh deploy`
4. Presto connector

#### Configuration
##### Initialization
1. Create user and database in pg for metadata.
开启服务:`sudo service postgresql restart`, 或者尝试以下方法:
```
查看命令：
ps aux | grep postgres
netstat -npl | grep postgres

重启命令：
#su - postgres
$pg_ctl restart
```

通过`sudo -u postgres psql`进入
```
CREATE USER paraflow WITH PASSWORD 'paraflow';
CREATE DATABASE paraflowmeta;
GRANT ALL ON DATABASE paraflowmeta TO paraflow;
```

#### Startup
1. Start Zookeeper
```
cd /home/iir/opt/zookeeper-3.4.13/conf
scp -r zoo.cfg iir@dbiir02:/home/iir/opt/zookeeper-3.4.13/conf/
cd /home/iir/opt/paraflow/sbin
./zookeeper-server.sh start
```
2. Start Kafka
```
cd /home/iir/opt/paraflow/sbin
./kafka-server.sh start
```
3. Start Posgresql
```
sudo -u postgres psql

```
4. Start Paraflow MetaServer
`./bin/paraflow-metaserver-start.sh [-daemon]`
5. Start Paraflow Loader
`./sbin/paraflow-loader.sh start`
6. Start Paraflow Collector
`./sbin/paraflow-collector.sh start`
7. Start Presto
```
cd /home/iir/opt/presto-server-0.192
./bin/presto --server localhost:8080 --catalog paraflow --schema test
```



## configuration
- 清空数据
```
// hadoop
./bin/hadoop fs -rm -r /paraflow/test/tpch/*
// PG
DROP DATABASE paraflowmeta;
```