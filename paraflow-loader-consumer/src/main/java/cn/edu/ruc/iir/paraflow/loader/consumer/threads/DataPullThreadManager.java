package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataPullThreadManager
{
    private final ExecutorService executorService;
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final int threadNum = config.getKafkaThreadNum();
    private DataPullThread[] threads = new DataPullThread[threadNum];

    private DataPullThreadManager()
    {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    private static class ConsumerThreadManagerHolder
    {
        private static final DataPullThreadManager instance = new DataPullThreadManager();
    }

    public static final DataPullThreadManager INSTANCE()
    {
        return ConsumerThreadManagerHolder.instance;
    }

    public void init(List<TopicPartition> topicPartitions)
    {
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new DataPullThread("kafka-thread" + i, topicPartitions);
        }
    }

    public void run()
    {
        for (DataPullThread thread : threads) {
            executorService.submit(thread);
        }
    }

    public void shutdown()
    {
        for (DataPullThread thread : threads) {
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
