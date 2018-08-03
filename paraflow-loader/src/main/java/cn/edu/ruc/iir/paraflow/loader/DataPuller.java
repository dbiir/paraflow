package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.Stats;
import com.conversantmedia.util.concurrent.ConcurrentQueue;
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
    private final DataTransformer transformer;
    private final ConcurrentQueue<ParaflowRecord> concurrentQueue;

    public DataPuller(String threadName,
                      int parallelism,
                      List<TopicPartition> topicPartitions,
                      Properties conf,
                      DataTransformer transformer,
                      ConcurrentQueue<ParaflowRecord> concurrentQueue)
    {
        super(threadName, parallelism);
        ParaflowKafkaConsumer kafkaConsumer = new ParaflowKafkaConsumer(topicPartitions, conf);
        this.consumer = kafkaConsumer.getConsumer();
        this.stats = new Stats(3000);
        this.transformer = transformer;
        this.concurrentQueue = concurrentQueue;
    }

    @Override
    public void run()
    {
        System.out.println(super.name + " started.");
        try {
            while (!isReadyToStop.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                int bytes = 0;
                int counter = 0;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] value = record.value();
                    // transform a kafka record into a paraflow record
                    ParaflowRecord paraflowRecord = transformer.transform(value, record.partition());
                    while (!concurrentQueue.offer(paraflowRecord)) {
                        // do nothing
                    }
                    // update statistics
                    bytes += value.length;
                    counter++;
                }
                stats.record(bytes, counter);
            }
        }
        catch (WakeupException e) {
            e.printStackTrace();
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
