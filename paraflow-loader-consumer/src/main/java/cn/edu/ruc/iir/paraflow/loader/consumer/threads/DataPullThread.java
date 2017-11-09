package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.KafkaConsumerClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class DataPullThread extends DataThread
{
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private final KafkaConsumerClient consumerClient = new KafkaConsumerClient();
    private List<TopicPartition> topicPartitions;

    public DataPullThread()
    {
        this("kafka-thread");
    }

    public DataPullThread(String threadName)
    {
        this.threadName = threadName;
    }

    public DataPullThread(String threadName, List<TopicPartition> topicPartitions)
    {
        this.threadName = threadName;
        this.topicPartitions = topicPartitions;
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
     * DataPullThread run() is used to poll message from kafka to buffer
     */
    @Override
    public void run()
    {
        while (true) {
            if (isReadyToStop) { //loop end condition
                System.out.println("Thread stop");
                consumerClient.close();
                return;
            }
                consumerClient.assign(topicPartitions);
                ConsumerRecords<Long, Message> consumerRecords = consumerClient.poll(config.getConsumerPollTimeout());
                for (ConsumerRecord<Long, Message> record : consumerRecords) {
                    System.out.println("record : " + record);
                    buffer.offer(record.value());
                    System.out.println("buffer size: " + buffer.size());
                }
        }
    }
}
