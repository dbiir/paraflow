package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class DataPuller
        extends Processor
{
    private static final Logger logger = LoggerFactory.getLogger(DataPuller.class);
    private final Consumer<byte[], byte[]> consumer;
    private final BlockingQueue<ParaflowSortedBuffer> blockingQueue;
    private final int sortedBufferCapacity;
    private final ParaflowRecord[][] fiberBuffers;
    private final int[] fiberIndices;
    private final Map<Integer, Integer> fiberMapping;
    private DataTransformer transformer;

    DataPuller(String threadName, String db, String table, int parallelism,
               List<TopicPartition> topicPartitions,
               Properties conf,
               String transformerClass,
               BlockingQueue<ParaflowSortedBuffer> blockingQueue,
               int sortedBufferCapacity)
    {
        super(threadName, db, table, parallelism);
        ParaflowKafkaConsumer kafkaConsumer = new ParaflowKafkaConsumer(topicPartitions, conf);
        this.consumer = kafkaConsumer.getConsumer();
        this.blockingQueue = blockingQueue;
        this.sortedBufferCapacity = sortedBufferCapacity;
        int partitionNum = topicPartitions.size();
        this.fiberBuffers = new ParaflowRecord[partitionNum][];
        this.fiberIndices = new int[partitionNum];
        this.fiberMapping = new HashMap<>(partitionNum);
        for (int i = 0; i < partitionNum; i++) {
            fiberMapping.put(topicPartitions.get(i).partition(), i);
        }
        this.transformer = null;
        try {
            Class clazz = DefaultLoader.class.getClassLoader().loadClass(transformerClass);
            this.transformer = (DataTransformer) clazz.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            logger.error(stringWriter.toString());
        }
    }

    @Override
    public void run()
    {
        if (this.transformer == null) {
            logger.error("No data transformer available.");
            return;
        }
        System.out.println(super.name + " started.");
        logger.info(super.name + " started.");
        while (!isReadyToStop.get()) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                for (TopicPartition topicPartition : records.partitions()) {
                    int partition = topicPartition.partition();
                    for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                        int partitionIndex = fiberMapping.get(partition);
                        int fiberIndex = fiberIndices[partitionIndex];
                        if (fiberIndex >= sortedBufferCapacity) {
                            ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(fiberBuffers[partitionIndex], partition);
                            try {
                                blockingQueue.put(sortedBuffer);
                                fiberIndices[partitionIndex] = 0;
                                fiberIndex = 0;
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        if (fiberBuffers[partitionIndex] == null) {
                            fiberBuffers[partitionIndex] = new ParaflowRecord[sortedBufferCapacity];
                        }
                        ParaflowRecord paraflowRecord = transformer.transform(record.value(), partition);
                        if (paraflowRecord != null) {
                            fiberBuffers[partitionIndex][fiberIndex] = paraflowRecord;
                            fiberIndices[partitionIndex] = fiberIndex + 1;
                        }
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
