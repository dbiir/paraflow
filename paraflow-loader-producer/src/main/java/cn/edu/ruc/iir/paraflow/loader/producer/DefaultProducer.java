package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.producer.buffer.BlockingQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.Utils;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultProducer implements Producer
{
    private final MetaClient metaClient;
    private final AdminClient kafkaAdminClient;
    private final Map<String, List<Function<Message, Boolean>>> filtersMap;
    private final BlockingQueueBuffer buffer = BlockingQueueBuffer.INSTANCE();
    private final long offerTimeout = ProducerConfig.INSTANCE().getBufferOfferTimeout();

    public DefaultProducer()
    {
        metaClient = new MetaClient(ProducerConfig.INSTANCE().getServerHost(),
                ProducerConfig.INSTANCE().getServerPort());
        Properties properties = new Properties();
        // todo set kafka admin props
        kafkaAdminClient = AdminClient.create(properties);
        filtersMap = new HashMap<>();
    }

    @Override
    public void send(String database, String table, Message message)
    {
        // todo get message topic
        message.setTopic("");
        List<Function<Message, Boolean>> filters = filtersMap.get(Utils.formTopicName(database, table));
        if (filters != null) {
            for (Function<Message, Boolean> func : filtersMap.get(database + "." + table)) {
                if (func.apply(message)) {
                    return;
                }
            }
        }
        while (true) {
            try {
                buffer.offer(message, offerTimeout);
                break;
            }
            catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public void createTopic(String topicName, int partitionsNum, short replcationFactor)
    {
        NewTopic newTopic = new NewTopic(topicName, partitionsNum, replcationFactor);
        kafkaAdminClient.createTopics(Collections.singletonList(newTopic));
    }

    @Override
    public StatusProto.ResponseStatus createUser(String userName, String password)
    {
        return metaClient.createUser(userName, password);
    }

    @Override
    public StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl)
    {
        return metaClient.createDatabase(databaseName, userName, locationUrl);
    }

    @Override
    public StatusProto.ResponseStatus createRegularTable(String dbName, String tblName, String userName, String locationUrl, String storageFormatName, ArrayList<String> columnName, ArrayList<String> columnType, ArrayList<String> dataType)
    {
        return metaClient.createRegularTable(dbName, tblName, userName, locationUrl, storageFormatName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName,
                                                       String tblName,
                                                       String userName,
                                                       String storageFormatName,
                                                       int fiberColIndex,
                                                       int timestampColIndex,
                                                       String fiberFuncName,
                                                       ArrayList<String> columnName,
                                                       ArrayList<String> columnType,
                                                       ArrayList<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColName, fiberFuncName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName,
                                                       String tblName,
                                                       String userName,
                                                       String locationUrl,
                                                       String storageFormatName,
                                                       int fiberColIndex,
                                                       int timestampColIndex,
                                                       String fiberFuncName,
                                                       ArrayList<String> columnName,
                                                       ArrayList<String> columnType,
                                                       ArrayList<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColName, fiberFuncName, columnName, columnType, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberFunc(String funcName, Function<String, Long> func) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput objOutput = new ObjectOutputStream(bos);
        objOutput.writeObject(func);
        objOutput.flush();
        return metaClient.createFiberFunc(funcName, bos.toByteArray());
    }

    @Override
    public void registerFilter(String database, String table, Function<Message, Boolean> filterFunc)
    {
        String key = Utils.formTopicName(database, table);
        if (filtersMap.containsKey(key)) {
            filtersMap.get(key).add(filterFunc);
        }
        else {
            List<Function<Message, Boolean>> funcList = new ArrayList<>();
            funcList.add(filterFunc);
            filtersMap.put(key, funcList);
        }
    }
}
