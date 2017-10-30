package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.producer.DefaultProducer;

/**
 * paraflow
 *
 * @author guodong
 */
public class ExampleProducer
{
    private void exampleTest(String consumerConfig, String metaServerConfig)
    {
        final DefaultProducer producer;
        try {
            producer = new DefaultProducer(consumerConfig);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        final String tblName = "exampleTbl";
        final String dbName = "exampleDb";
        final int fiberKeyIndex = 0;
//        String topicName = FormTopicName.formTopicName(dbName, tblName);
//        producer.createTopic(topicName, 1, (short) 1);
        System.out.println("Created topic " + FormTopicName.formTopicName(dbName, tblName));
        SerializableFunction<String, Long> func = (v) -> Long.parseLong(v) % 1000;
        producer.registerFiberFunc(dbName, tblName, func);
        for (int i = 0; i < 50; i++) {
//            fiberKeyIndex = i;
            long ts = System.currentTimeMillis();
            String[] content = {String.valueOf(i), String.valueOf(i * 2), "alice" + i, String.valueOf(ts)};
            Message msg = new Message(fiberKeyIndex, content, ts);
            producer.send(dbName, tblName, msg);
        }
        System.out.println("Done with sending");
        producer.shutdown();
    }

    public static void main(String[] args)
    {
        ExampleProducer producer = new ExampleProducer();
        producer.exampleTest(args[0], args[1]);
    }
}
