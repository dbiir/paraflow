package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.producer.DefaultProducer;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class ExampleProducer
{
    private MetaClient metaClient = new MetaClient("127.0.0.1", 10012);

    private final String tblName = "exampletbl";
    private final String dbName = "exampledb";
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
//        String topicName = FormTopicName.formTopicName(dbName, tblName);
//        producer.createTopic(topicName, 1, (short) 1);
        SerializableFunction<String, Integer> func = (v) -> Integer.parseInt(v) % 1000;
        producer.registerFiberFunc(dbName, tblName, func);
        for (int i = 0; i < 1500000; i++) {
            long ts = System.currentTimeMillis();
            String[] content = {String.valueOf(i), String.valueOf(i * 2), "alice" + i, String.valueOf(ts)};
            Message msg = new Message(fiberKeyIndex, content, ts);
            producer.send(dbName, tblName, msg);
        }
        System.out.println("Done with sending");
        try {
            Thread.sleep(20000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }

    @Test
    public void createUser()
    {
        metaClient.createUser("alice", "alice");
    }

    @Test
    public void createDatabase()
    {
        metaClient.createDatabase(dbName, "alice");
    }

    @Test
    public void createSF()
    {
        metaClient.createStorageFormat("parquet", "none", "org.apache.parquet.Parquet");
    }

    @Test
    public void createFiberFunc()
    {
        String string = "I am a girl";
        byte[] funcContent = string.getBytes();
        metaClient.createFunc("function0", funcContent);
    }

    @Test
    public void createTable()
    {
        final String userName = "alice";
        final String storageFormatName = "parquet";
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
        StatusProto.ResponseStatus status = metaClient.createFiberTable(
                dbName,
                tblName,
                userName,
                storageFormatName,
                1,
                "function0",
                3,
                columnName,
                dataType);
        System.out.println(status.getStatusValue());
    }

    public static void main(String[] args)
    {
        ExampleProducer producer = new ExampleProducer();
        producer.exampleTest(args[0], args[1]);
    }
}
