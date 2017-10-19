package cn.edu.ruc.iir.paraflow.loader.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.function.Function;

public interface Consumer
{
    void consume(LinkedList<TopicPartition> topicPartitions);

    void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc);

    void shutdown();
}
