package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;

public class ConsumerThread implements Runnable
{
    private final String threadName;
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private final KafkaConsumerClient consumerClient = new KafkaConsumerClient();
    private final MetaClient metaClient = new MetaClient(config.getMetaServerHost(), config.getMetaServerPort());
    private LinkedList<TopicPartition> topicPartitions;

    private boolean isReadyToStop = false;

    public ConsumerThread()
    {
        this("kafka-thread");
    }

    public ConsumerThread(String threadName)
    {
        this.threadName = threadName;
    }

    public ConsumerThread(String threadName, LinkedList<TopicPartition> topicPartitions)
    {
        this.threadName = threadName;
        this.topicPartitions = topicPartitions;
    }

    public String getName()
    {
        return threadName;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */

    /**
     * ConsumerThread run() is used to poll message from kafka to buffer
     */
    @Override
    public void run()
    {
        while (true) {
            if (isReadyToStop && buffer.remainingCapacity() != 0) { //loop end condition
                System.out.println("Thread stop");
                consumerClient.close();
                return;
            }
            try {
                consumerClient.assign(topicPartitions);
                ConsumerRecords<Long, Message> consumerRecords = consumerClient.poll(config.getConsumerPollTimeout());
                for (ConsumerRecord<Long, Message> record : consumerRecords) {
                    buffer.put(record.value());
                }
            }
            catch (InterruptedException ignored) {
                // if poll nothing, enter next loop
            }
        }
    }

    private void readyToStop()
    {
        this.isReadyToStop = true;
    }

    public void shutdown()
    {
        try {
            readyToStop();
            metaClient.shutdown(config.getMetaClientShutdownTimeout());
        }
        catch (InterruptedException e) {
            readyToStop();
            metaClient.shutdownNow();
        }
    }
}
