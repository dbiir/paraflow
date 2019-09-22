# ParaFlow

ParaFlow is an interactive analysis system for OLAP developed at [DBIIR Lab @ RUC](http://iir.ruc.edu.cn).

#### Install & Deploy
##### Hadoop
Hadoop file system is required.
##### Zookeeper-3.4.13
This is required by Kafka.
what need to deploy is simply config the cluster ip and port;
##### Kafka-2.11_1.11
##### Postgresql-9.5
##### Presto-0.192
##### Paraflow
1. MetaServer(one node)
2. Loader [cn.edu.ruc.iir.paraflow.example.loader.BasicLoader]

    config the ./paraflow-loader.sh then:

    `./sbin/paraflow-loader.sh deploy`
3. Collector [cn.edu.ruc.iir.paraflow.example.loader.BasicCollector]
    
    config the ./paraflow-collector.sh then:    
    `./sbin/paraflow-collector.sh deploy`
4. Presto connector

#### Configuration
##### Initialization
1. Create user and database in pg for metadata.

`CREATE USER paraflow WITH PASSWORD 'paraflow'`;
`CREATE DATABASE paraflowmeta`;
`GRANT ALL ON DATABASE paraflowmeta TO paraflow`.

#### Startup
1. Start Zookeeper cluster
2. Start Kafka
3. Start PostgreSql
4. Start Paraflow MetaServer
`./bin/paraflow-metaserver-start.sh [-daemon]`
5. Start Paraflow Loader
`./sbin/paraflow-loader.sh start`
6. Start Paraflow Collector
`./sbin/paraflow-collector.sh start`
7. Start Presto cluster or single node to execute queries;
