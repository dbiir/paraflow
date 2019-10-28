package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

import java.util.List;

/**
 * paraflow collector
 * @description create topic in kafka and import data
 * @author guodong
 */
public interface Collector<T>
{
    void collect(DataSource dataSource, int keyIdx, int timeIdx, ParaflowFiberPartitioner partitioner, MessageSerializationSchema<T> serializer, DataSink dataSink);

    void deleteTopic(String topicName);

    void createTopic(String topicName, int partitionsNum, short replicationFactor);

    boolean existsDatabase(String db);

    boolean existsTable(String db, String table);

    StatusProto.ResponseStatus createUser(String userName, String password);

    StatusProto.ResponseStatus createDatabase(String databaseName);

    StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl);

    StatusProto.ResponseStatus createTable(String dbName,
                                           String tblName,
                                           String storageFormatName,
                                           int fiberColIndex,
                                           int timestampColIndex,
                                           String fiberPartitioner,
                                           List<String> columnName,
                                           List<String> dataType);

    StatusProto.ResponseStatus createTable(String dbName,
                                           String tblName,
                                           String userName,
                                           String storageFormatName,
                                           int fiberColIndex,
                                           int timestampColIndex,
                                           String fiberFuncName,
                                           List<String> columnName,
                                           List<String> dataType);

    void shutdown();
}
