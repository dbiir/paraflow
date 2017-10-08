package cn.edu.ruc.iir.paraflow.loader.producer.threads;

import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class ThreadManager
{
    private final ExecutorService executorService;
    private final ProducerConfig config = ProducerConfig.INSTANCE();
    private final int threadNum = config.getKafkaThreadNum();
    private KafkaThread[] threads = new KafkaThread[threadNum];

    private static class ThreadManagerHolder
    {
        private static final ThreadManager instance = new ThreadManager();
    }

    private ThreadManager()
    {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    public static final ThreadManager INSTANCE()
    {
        return ThreadManagerHolder.instance;
    }

    public void init()
    {
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new KafkaThread("kafka-thread" + i);
        }
    }

    public void run()
    {
        for (KafkaThread thread : threads) {
            executorService.submit(thread);
        }
    }

    public void shutdown()
    {
        for (KafkaThread thread : threads) {
            thread.shutdown();
        }
        try {
            executorService.awaitTermination(config.getProducerShutdownTimeout(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            executorService.shutdown();
        }
    }
}
