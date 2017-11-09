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
public class ProducerThreadManager
{
    private final ExecutorService executorService;
    private final ProducerConfig config = ProducerConfig.INSTANCE();
    private final int threadNum = config.getKafkaThreadNum();
    private ProducerThread[] threads = new ProducerThread[threadNum];

    private ProducerThreadManager()
    {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    private static class ThreadManagerHolder
    {
        private static final ProducerThreadManager instance = new ProducerThreadManager();
    }

    public static final ProducerThreadManager INSTANCE()
    {
        return ThreadManagerHolder.instance;
    }

    public void init()
    {
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new ProducerThread("kafka-thread" + i);
        }
    }

    public void run()
    {
        for (ProducerThread thread : threads) {
            executorService.submit(thread);
        }
        try {
            for (ProducerThread thread : threads) {
                thread.join();
            }
        }
        catch (InterruptedException e) {
            //
        }
    }

    public void shutdown()
    {
        for (ProducerThread thread : threads) {
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
