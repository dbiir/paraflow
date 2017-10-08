package cn.edu.ruc.iir.paraflow.loader.producer.utils;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * paraflow
 *
 * @author guodong
 */
public class KafkaProducerClient
{
    private final Producer<Long, Message> producer;

    public KafkaProducerClient()
    {
        Properties props = new Properties();
        ProducerConfig config = ProducerConfig.INSTANCE();
        props.put("bootstrap.servers", config.getKafkaBootstrapServers());
        props.put("acks", config.getKafkaAcks());
        props.put("retries", config.getKafkaRetries());
        props.put("batch.size", config.getKafkaBatchSize());
        props.put("linger.ms", config.getKafkaLingerMs());
        props.put("buffer.memory", config.getKafkaBufferMem());
        props.put("key.serializer", config.getKafkaKeySerializerClass());
        props.put("value.serializer", config.getKafkaValueSerializerClass());
        props.put("partitioner.class", config.getKafkaPartitionerClass());
        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, long key, Message message)
    {
        producer.send(new ProducerRecord<>(topic, key, message));
    }

    public void close()
    {
        producer.close();
    }
}
