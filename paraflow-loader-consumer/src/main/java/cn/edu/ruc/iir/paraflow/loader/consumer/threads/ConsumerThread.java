package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
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
            if (isReadyToStop) { //loop end condition
                System.out.println("Thread stop");
                consumerClient.close();
                return;
            }
                consumerClient.assign(topicPartitions);
                ConsumerRecords<Long, Message> consumerRecords = consumerClient.poll(config.getConsumerPollTimeout());
                for (ConsumerRecord<Long, Message> record : consumerRecords) {
                    System.out.println("ConsumerThread run() : record : " + record);
                    buffer.offer(record.value());
                    System.out.println("buffer size: " + buffer.size());
                }
//                LinkedList<Message> messages = new LinkedList<>();
//                buffer.drainTo(messages);
//                int i = 0;
//                for (Message message : messages) {
//                    System.out.println("i = " + i++);
//                    System.out.println("ConsumerThread : run() :message :" + message);
//                }
        }
    }

    private void readyToStop()
    {
        this.isReadyToStop = true;
    }

    public void shutdown()
    {
            readyToStop();
    }
}
