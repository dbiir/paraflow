package cn.edu.ruc.iir.paraflow.loader.producer.threads;

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
