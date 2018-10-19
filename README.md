# ParaFlow

ParaFlow is an interactive analysis system for OLAP developed at [DBIIR Lab @ RUC](http://iir.ruc.edu.cn).

#### Installation
##### Zookeeper
##### Kafka-1.11
##### Postgresql-9.5
##### Paraflow
1. MetaServer
2. Loader
3. Collector
4. Presto connector

#### Configuration
##### Initialization
1. Create user and database in pg for metadata.

`CREATE USER paraflow WITH PASSWORD 'paraflow'`;
`CREATE DATABASE paraflowmeta`;
`GRANT ALL ON DATABASE paraflowmeta TO paraflow`.

#### Startup
1. Start Zookeeper
2. Start Kafka
3. Start Posgresql
4. Start Paraflow MetaServer
`./bin/paraflow-metaserver-start.sh [-daemon]`
5. Start Paraflow Loader
6. Start Paraflow Collector
7. Start Presto
