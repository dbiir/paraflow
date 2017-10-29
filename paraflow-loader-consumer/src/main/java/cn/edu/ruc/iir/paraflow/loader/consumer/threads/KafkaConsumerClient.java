package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.Properties;

public class KafkaConsumerClient
{
    private final Consumer<Long, Message> consumer;

    public KafkaConsumerClient()
    {
        Properties props = new Properties();
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        props.put("bootstrap.servers", config.getKafkaBootstrapServers());
        props.put("acks", config.getKafkaAcks());
        props.put("retries", config.getKafkaRetries());
        props.put("batch.size", config.getKafkaBatchSize());
        props.put("buffer.memory", config.getKafkaBufferMem());
        props.put("key.deserializer", config.getKafkaKeyDeserializerClass());
        props.put("value.deserializer", config.getKafkaValueDeserializerClass());
        consumer = new KafkaConsumer<>(props);
    }

    public void assign(LinkedList<TopicPartition> topicPartitions)
    {
        consumer.assign(topicPartitions);
    }

    public ConsumerRecords<Long, Message> poll(long pollTimeout)
    {
        return consumer.poll(pollTimeout);
    }

    public void close()
    {
        consumer.close();
    }
}
