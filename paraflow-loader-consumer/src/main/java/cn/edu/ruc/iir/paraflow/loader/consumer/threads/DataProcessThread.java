package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.TopicFiber;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferPool;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;

import java.util.List;

public class DataProcessThread extends DataThread
{
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private final BufferPool bufferPool;

    public DataProcessThread(String threadName, List<TopicFiber> topicFibers)
    {
        super(threadName);
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        long blockSize = config.getBufferPoolSize();
        this.bufferPool = new BufferPool(topicFibers, blockSize, blockSize);
    }

    /**
     * DataProcessThread run() is used to poll message from consumer buffer
     */
    @Override
    public void run()
    {
        System.out.println(threadName + " started.");
        try {
            while (true) {
                if (isReadyToStop && buffer.isEmpty()) { //loop end condition
                    System.out.println("Thread stop");
                    return;
                }
                try {
                    Message message = buffer.poll(1000);
                    if (message != null) {
                        bufferPool.add(message);
//                        System.out.println("Processing message");
                    }
                    else {
                        System.out.println("Null message during processing");
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
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
