package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.producer.DefaultProducer;
import cn.edu.ruc.iir.paraflow.loader.producer.Producer;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.Utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class ExampleProducer
{
    private void exampleTest(String config)
    {
        final Producer producer;
        try {
            producer = new DefaultProducer(config);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        final String tblName = "exampleTbl";
        final String dbName = "exampleDb";
        final int fiberKeyIndex = 0;
        producer.createTopic(Utils.formTopicName(dbName, tblName), 100, (short) 1);
        System.out.println("Created topic " + Utils.formTopicName(dbName, tblName));
        SerializableFunction<String, Long> func = (v) -> Long.parseLong(v) % 1000;
        producer.registerFiberFunc(dbName, tblName, func);

        for (int i = 0; i < 10000; i++) {
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
        producer.exampleTest(args[0]);
    }
}
