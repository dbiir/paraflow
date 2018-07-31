package cn.edu.ruc.iir.paraflow.loader.threads;

import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;

public class DataPullThread
        extends DataThread
{
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final Consumer<byte[], byte[]> consumer;
    private List<TopicPartition> topicPartitions;

    public DataPullThread(String threadName, List<TopicPartition> topicPartitions)
    {
        super(threadName);
        this.topicPartitions = topicPartitions;
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getKafkaBootstrapServers());
        properties.put("group.id", config.getGroupId());
        properties.put("enable.auto.commit", true);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run()
    {
        System.out.println(threadName + " started.");
        long counter = 0;
        long bytes = 0;
        long start = System.currentTimeMillis();
        try {
            while (true) {
                if (isReadyToStop) { //loop end condition
                    System.out.println("Thread stop");
                    consumer.close();
                    return;
                }
                consumer.assign(topicPartitions);
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(config.getConsumerPollTimeout());
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    counter++;
                    bytes += record.value().length;
                    long current = System.currentTimeMillis();
                    long interval = current - start;
                    if (interval >= 3000) {
                        double speed = 1.0 * bytes / interval;
                        System.out.println("Pulling records: " + counter + " at the speed of " + speed);
                        counter = 0;
                        bytes = 0;
                        start = System.currentTimeMillis();
                    }
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    consumer.wakeup();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println(threadName + " stopped");
        }
    }
}
