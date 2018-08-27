package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.Stats;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class DataPuller
        extends Processor
{
    private static final Logger logger = LoggerFactory.getLogger(DataPuller.class);
    private final Consumer<byte[], byte[]> consumer;
    private final Stats stats;
    private final DataTransformer transformer;
    private final BlockingQueue<ParaflowSortedBuffer> blockingQueue;
    private final int sortedBufferCapacity;
    private final ParaflowRecord[][] fiberBuffers;
    private final int[] fiberIndices;

    DataPuller(String threadName, String db, String table, int parallelism,
               List<TopicPartition> topicPartitions,
               Properties conf,
               DataTransformer transformer,
               BlockingQueue<ParaflowSortedBuffer> blockingQueue,
               int sortedBufferCapacity)
    {
        super(threadName, db, table, parallelism);
        ParaflowKafkaConsumer kafkaConsumer = new ParaflowKafkaConsumer(topicPartitions, conf);
        this.consumer = kafkaConsumer.getConsumer();
        this.stats = new Stats(3000);
        this.transformer = transformer;
        this.blockingQueue = blockingQueue;
        this.sortedBufferCapacity = sortedBufferCapacity;
        int partitionNum = topicPartitions.size();
        this.fiberBuffers = new ParaflowRecord[partitionNum][];
        this.fiberIndices = new int[partitionNum];
    }

    @Override
    public void run()
    {
        System.out.println(super.name + " started.");
        logger.info(super.name + " started.");
        while (!isReadyToStop.get()) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                for (TopicPartition topicPartition : records.partitions()) {
                    int partition = topicPartition.partition();
                    for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                        int fiberIndex = fiberIndices[partition];
                        if (fiberIndex >= sortedBufferCapacity) {
                            ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(fiberBuffers[partition], partition);
                            try {
                                blockingQueue.put(sortedBuffer);
                                fiberIndices[partition] = 0;
                                fiberIndex = 0;
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        stats.record(record.value().length, 1);
                        if (fiberBuffers[partition] == null) {
                            fiberBuffers[partition] = new ParaflowRecord[sortedBufferCapacity];
                        }
                        fiberBuffers[partition][fiberIndex] = transformer.transform(record.value(), partition);
                        fiberIndices[partition] = fiberIndex + 1;
                    }
                    consumer.commitAsync();
                }
            }
            catch (WakeupException e) {
                System.out.println(super.name + " wakes up.");
                logger.info(super.name + " wakes up.");
                if (isReadyToStop.get()) {
                    consumer.close();
                    System.out.println(super.name + " stopped.");
                    logger.info(super.name + " stopped.");
                    break;
                }
            }
        }
    }

    @Override
    public void stop()
    {
        isReadyToStop.set(true);
        consumer.wakeup();
    }
}
