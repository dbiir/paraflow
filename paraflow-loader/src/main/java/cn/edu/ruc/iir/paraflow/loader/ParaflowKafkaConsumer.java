package cn.edu.ruc.iir.paraflow.loader;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.List;
import java.util.Properties;

public class ParaflowKafkaConsumer
{
    private final Consumer<byte[], byte[]> consumer;

    public ParaflowKafkaConsumer(List<TopicPartition> topicPartitions, Properties config)
    {
        // set the consumer configuration properties for kafka record key and value serializers
        if (!config.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        if (!config.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        if (!config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be specified in the config");
        }
        this.consumer = new KafkaConsumer<>(config);
        this.consumer.assign(topicPartitions);
    }

    public Consumer<byte[], byte[]> getConsumer()
    {
        return this.consumer;
    }
}
