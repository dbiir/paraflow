package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

import java.util.List;
import java.util.function.Function;

/**
 * paraflow connector
 *
 * @author guodong
 */
public interface Collector<T>
{
    void collect(DataSource dataSource, int keyIdx, int timeIdx, ParaflowFiberPartitioner partitioner, MessageSerializationSchema<T> serializer, DataSink dataSink);

    void deleteTopic(String topicName);

    void createTopic(String topicName, int partitionsNum, short replicationFactor);

    StatusProto.ResponseStatus createUser(String userName, String password);

    StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl);

    StatusProto.ResponseStatus createRegularTable(String dbName,
                                                  String tblName,
                                                  String userName,
                                                  String locationUrl,
                                                  String storageFormatName,
                                                  List<String> columnName,
                                                  List<String> dataType);

    StatusProto.ResponseStatus createFiberTable(String dbName,
                                                String tblName,
                                                String userName,
                                                String storageFormatName,
                                                int fiberColIndex,
                                                int timestampColIndex,
                                                String fiberFuncName,
                                                List<String> columnName,
                                                List<String> dataType);

    StatusProto.ResponseStatus createFiberTable(String dbName,
                                                String tblName,
                                                String userName,
                                                String locationUrl,
                                                String storageFormatName,
                                                int fiberColIndex,
                                                int timestampColIndex,
                                                String fiberFuncName,
                                                List<String> columnName,
                                                List<String> dataType);

    void registerFiberFunc(String database, String table, Function<String, Integer> fiberFunc);

    void registerFilter(String database, String table, Function<Message, Boolean> filterFunc);

    void registerTransformer(String database, String table, Function<Message, Message> transformerFunc);

    void shutdown();
}