package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.Stats;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.Properties;

public class DataPuller
        extends Processor
{
    private final Consumer<byte[], byte[]> consumer;
    private final Stats stats;

    public DataPuller(String threadName, List<TopicPartition> topicPartitions, Properties conf)
    {
        super(threadName);
        ParaflowKafkaConsumer kafkaConsumer = new ParaflowKafkaConsumer(topicPartitions, conf);
        this.consumer = kafkaConsumer.getConsumer();
        this.stats = new Stats(3000);
    }

    @Override
    public void run()
    {
        System.out.println(threadName + " started.");
        try {
            while (!isReadyToStop.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    stats.record(record.value().length, 1);
                }
            }
        }
        catch (WakeupException e) {
            if (isReadyToStop.get()) {
                System.out.println("Thread stop");
                consumer.close();
            }
        }
        finally {
            consumer.close();
        }
    }

    @Override
    public void stop()
    {
        isReadyToStop.set(true);
        consumer.wakeup();
    }
}
