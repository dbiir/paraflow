package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadManager
{
    private final ExecutorService executorService;
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final int threadNum = config.getKafkaThreadNum();
    private ConsumerThread[] threads = new ConsumerThread[threadNum];

    private ConsumerThreadManager()
    {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    private static class ConsumerThreadManagerHolder
    {
        private static final ConsumerThreadManager instance = new ConsumerThreadManager();
    }

    public static final ConsumerThreadManager INSTANCE()
    {
        return ConsumerThreadManagerHolder.instance;
    }

    public void init(LinkedList<TopicPartition> topicPartitions)
    {
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new ConsumerThread("kafka-thread" + i, topicPartitions);
        }
    }

    public void run()
    {
        for (ConsumerThread thread : threads) {
            executorService.submit(thread);
        }
    }

    public void shutdown()
    {
        for (ConsumerThread thread : threads) {
            thread.shutdown();
        }
        try {
            executorService.awaitTermination(config.getConsumerShutdownTimeout(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            executorService.shutdown();
        }
    }
}
