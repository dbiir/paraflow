package cn.edu.ruc.iir.paraflow.loader.producer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.buffer.BlockingQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.KafkaProducerClient;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

import java.util.Optional;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class ProducerThread extends Thread
{
    private final String threadName;
    private final ProducerConfig config = ProducerConfig.INSTANCE();
    private final BlockingQueueBuffer buffer = BlockingQueueBuffer.INSTANCE();
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private final KafkaProducerClient producerClient = new KafkaProducerClient();
    private final MetaClient metaClient = new MetaClient(config.getMetaServerHost(), config.getMetaServerPort());

    private boolean isReadyToStop = false;

    public ProducerThread()
    {
        this("kafka-thread");
    }

    public ProducerThread(String threadName)
    {
        this.threadName = threadName;
    }

    public String getThreadName()
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
                Message msg = buffer.poll(config.getBufferPollTimeout());
                if (msg == null) {
                    // ignore it
                    continue;
                }
                System.out.println("[msg]: " + msg);
                if (msg.getTopic().isPresent()) {
                    String topic = msg.getTopic().get();
                    Optional<Function<String, Integer>> function = funcMapBuffer.get(topic);
                    function.ifPresent(stringLongFunction -> {
                        int fiberId = stringLongFunction.apply(msg.getKey());
                        msg.setFiberId(fiberId);
                        producerClient.send(topic, fiberId, msg);
                    });
                    // else ignore this message
                }
                // else ignore this message
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

    void shutdown()
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
