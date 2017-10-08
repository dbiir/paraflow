package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public interface Producer
{
    void send(String database, String table, Message message);

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

    StatusProto.ResponseStatus createFiberFunc(String funcName, SerializableFunction<String, Long> func) throws IOException;

    void registerFilter(String database, String table, Function<Message, Boolean> filterFunc);

    void registerTransformer(String database, String table, Function<Message, Message> transformerFunc);
}
