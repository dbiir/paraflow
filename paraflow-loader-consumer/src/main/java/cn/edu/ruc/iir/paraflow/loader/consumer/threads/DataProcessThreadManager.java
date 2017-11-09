package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataProcessThreadManager
{
    private final ExecutorService executorService;
    private final ConsumerConfig config = ConsumerConfig.INSTANCE();
    private final int threadNum = config.getKafkaThreadNum();
    private DataProcessThread[] threads = new DataProcessThread[threadNum];

    private DataProcessThreadManager()
    {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    private static class DataProcessThreadManagerHolder
    {
        private static final DataProcessThreadManager instance = new DataProcessThreadManager();
    }

    public static final DataProcessThreadManager INSTANCE()
    {
        return DataProcessThreadManager.DataProcessThreadManagerHolder.instance;
    }

    public void init(List<TopicPartition> topicPartitions)
    {
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new DataProcessThread("kafka-thread" + i, topicPartitions);
        }
    }

    public void run()
    {
        for (DataProcessThread thread : threads) {
            executorService.submit(thread);
        }
    }

    public void shutdown()
    {
        for (DataProcessThread thread : threads) {
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
