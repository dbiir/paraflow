package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferPool;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class DataProcessThread extends DataThread
{
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private final BufferPool bufferPool;

    public DataProcessThread(String threadName, List<TopicPartition> topicPartitions)
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        this.threadName = threadName;
        long blockSize = config.getBufferOfferBlockSize();
        this.bufferPool = new BufferPool(topicPartitions, blockSize, blockSize);
    }

    /**
     * DataProcessThread run() is used to poll message from consumer buffer
     */
    @Override
    public void run()
    {
        while (true) {
            if (isReadyToStop && buffer.isEmpty()) { //loop end condition
                System.out.println("Thread stop");
                return;
            }
            Message message = buffer.poll();
            if (message != null) {
                bufferPool.add(message);
            }
        }
    }
}
