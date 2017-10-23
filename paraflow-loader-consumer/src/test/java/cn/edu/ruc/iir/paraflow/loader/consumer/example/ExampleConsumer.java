package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.func.DeserializableFunction;
import cn.edu.ruc.iir.paraflow.loader.consumer.DefaultConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;

public class ExampleConsumer
{
    private void exampleTest(String configPath)
    {
        final DefaultConsumer consumer;
        LinkedList<TopicPartition> topicPartitions = new LinkedList<>();
        for (int i = 0; i<100; i ++) {
            TopicPartition topicPartition = new TopicPartition("exampleDb.exampleTbl", i);
            topicPartitions.add(topicPartition);
        }
        final String dbName = "exampleDb";
        final String tblName = "exampleTbl";
        try {
            consumer = new DefaultConsumer(configPath);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        DeserializableFunction<String, Long> func = (v) -> Long.parseLong(v) % 1000;
        consumer.registerFiberFunc(dbName, tblName, func);
        consumer.consume(topicPartitions);
        System.out.println("Done with consuming");
        consumer.shutdown();
    }
    public static void main(String[] args)
    {
        ExampleConsumer consumer = new ExampleConsumer();
        consumer.exampleTest(args[0]);
    }
}
