package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.producer.DefaultProducer;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;

import java.util.LinkedList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class ExampleProducer
{
    private final String tblName = "exampleTbl";
    private final String dbName = "exampleDb";
    private MetaConfig metaConfig = MetaConfig.INSTANCE();
    private void exampleTest(String producerConfig, String metaServerConfig)
    {
        final DefaultProducer producer;
        try {
            metaConfig.init(metaServerConfig);
            metaConfig.validate();
            producer = new DefaultProducer(producerConfig);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        final int fiberKeyIndex = 0;
//        createDbTbl();
//        String topicName = FormTopicName.formTopicName(dbName, tblName);
//        producer.createTopic(topicName, 1, (short) 1);
//        System.out.println("Created topic " + FormTopicName.formTopicName(dbName, tblName));
        SerializableFunction<String, Long> func = (v) -> Long.parseLong(v) % 1000;
        producer.registerFiberFunc(dbName, tblName, func);
        for (int i = 0; i < 30; i++) {
//            fiberKeyIndex = i;
            long ts = System.currentTimeMillis();
            String[] content = {String.valueOf(i), String.valueOf(i * 2), "alice" + i, String.valueOf(ts)};
            Message msg = new Message(fiberKeyIndex, content, ts);
            producer.send(dbName, tblName, msg);
        }
        System.out.println("Done with sending");
//        try {
//            Thread.sleep(10000);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        producer.shutdown();
    }

    private void createDbTbl()
    {
        final MetaClient metaClient;
        ProducerConfig config = ProducerConfig.INSTANCE();
            metaClient = new MetaClient(config.getMetaServerHost(),
                    config.getMetaServerPort());
        MetaProto.DbParam dbParam = metaClient.getDatabase(dbName);
        if (dbParam.getIsEmpty()) {
            final String userName = metaConfig.getDBUser();
            final String storageFormatName = "StorageFormatName";
            final List<String> columnName = new LinkedList<>();
            final List<String> dataType = new LinkedList<>();
            columnName.add("stu_id");
            columnName.add("class_id");
            columnName.add("alice_id");
            columnName.add("time");
            dataType.add("int");
            dataType.add("int");
            dataType.add("varchar(10)");
            dataType.add("bigint");
            System.out.println("userName : " + userName);
            metaClient.createDatabase(dbName, userName);
            metaClient.createRegularTable(
                    dbName,
                    tblName,
                    userName,
                    storageFormatName,
                    columnName,
                    dataType);
        }
    }

    public static void main(String[] args)
    {
        ExampleProducer producer = new ExampleProducer();
        producer.exampleTest(args[0], args[1]);
    }
}
