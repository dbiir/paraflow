package cn.edu.ruc.iir.paraflow.loader.producer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.producer.buffer.BlockingQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

/**
 * paraflow
 *
 * @author guodong
 */
public class KafkaThread implements Runnable
{
    private final String threadName;
    private final ProducerConfig config = ProducerConfig.INSTANCE();
    private boolean isReadyToStop = false;
    private BlockingQueueBuffer buffer = BlockingQueueBuffer.INSTANCE();
    private KafkaProducerClient producerClient = new KafkaProducerClient();
    private final MetaClient metaClient = new MetaClient(config.getMetaServerHost(), config.getMetaServerPort());

    public KafkaThread()
    {
        this("kafka-thread");
    }

    public KafkaThread(String threadName)
    {
        this.threadName = threadName;
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
    @Override
    public void run()
    {
        while (true) {
            if (isReadyToStop && (buffer.poll() == null)) {
                System.out.println("Thread stop");
                producerClient.close();
                return;
            }
            try {
                Message msg = buffer.poll(ProducerConfig.INSTANCE().getBufferPollTimeout());
                producerClient.send(msg.getTopic(), msg.getKey(), msg);
            }
            catch (InterruptedException e) {
                // nothing to do
            }
        }
    }

    public void readyToStop()
    {
        this.isReadyToStop = true;
    }
}
