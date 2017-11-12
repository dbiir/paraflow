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

    public DataPullThread(String threadName)
    {
        super(threadName);
    }

    public DataPullThread(String threadName, List<TopicPartition> topicPartitions)
    {
        super(threadName);
        this.topicPartitions = topicPartitions;
    }

    @Override
    public void run()
    {
        System.out.println(threadName + " started.");
        try {
            while (true) {
                if (isReadyToStop) { //loop end condition
                    System.out.println("Thread stop");
                    consumerClient.close();
                    return;
                }
                consumerClient.assign(topicPartitions);
                ConsumerRecords<Long, Message> consumerRecords = consumerClient.poll(config.getConsumerPollTimeout());
                for (ConsumerRecord<Long, Message> record : consumerRecords) {
                    buffer.offer(record.value());
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    consumerClient.wakeup();
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
