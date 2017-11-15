package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.TopicFiber;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataThreadManager
{
    private final ExecutorService executorService;
    private final ConsumerConfig consumerConfig;
    private final int pullThreadNum;
    private final int processThreadNum;
    private final int flushThreadNum;

    private final DataPullThread[] pullThreads;
    private final DataProcessThread[] processThreads;
    private final DataFlushThread[] flushThreads;

    private DataThreadManager()
    {
        this.consumerConfig = ConsumerConfig.INSTANCE();
        this.pullThreadNum = consumerConfig.getDataPullThreadNum();
        this.processThreadNum = consumerConfig.getDataProcessThreadNum();
        this.flushThreadNum = consumerConfig.getDataFlushThreadNum();
        this.executorService = Executors.newCachedThreadPool();

        this.pullThreads = new DataPullThread[pullThreadNum];
        this.processThreads = new DataProcessThread[processThreadNum];
        this.flushThreads = new DataFlushThread[flushThreadNum];
    }

    private static class DataThreadManagerHolder
    {
        private static final DataThreadManager instance = new DataThreadManager();
    }

    public static final DataThreadManager INSTANCE()
    {
        return DataThreadManagerHolder.instance;
    }

    public void init(List<TopicPartition> topicPartitions, List<TopicFiber> topicFibers)
    {
        // assign partitions between pull thread by round robin
        List<TopicPartition>[] fiberPartitions = new List[pullThreadNum];
        for (int i = 0; i < pullThreadNum; i++) {
            fiberPartitions[i] = new LinkedList<>();
        }
        for (int i = 0; i < topicPartitions.size(); i++) {
            fiberPartitions[i % pullThreadNum].add(topicPartitions.get(i));
        }
        for (int i = 0; i < pullThreadNum; i++) {
            pullThreads[i] = new DataPullThread("data-pull-thread-" + i, fiberPartitions[i]);
        }
        // init process thread
        for (int i = 0; i < processThreadNum; i++) {
            processThreads[i] = new DataProcessThread("data-process-thread-" + i, topicFibers);
        }
        // init flush thread
        for (int i = 0; i < flushThreadNum; i++) {
            flushThreads[i] = new PlainTextFlushThread("data-flush-thread-" + i);
            //flushThreads[i] = new OrcFlushThread("data-flush-thread-" + i);
            //flushThreads[i] = new ParquetFlushThread("data-flush-thread-" + i);
        }
    }

    public void run()
    {
        for (int i = 0; i < pullThreadNum; i++) {
            executorService.submit(pullThreads[i]);
        }
        for (int i = 0; i < processThreadNum; i++) {
            executorService.submit(processThreads[i]);
        }
        for (int i = 0; i < flushThreadNum; i++) {
            executorService.submit(flushThreads[i]);
        }
    }

    public void shutdown()
    {
        for (DataPullThread pullThread : pullThreads) {
            pullThread.shutdown();
        }
        for (DataProcessThread processThread : processThreads) {
            processThread.shutdown();
        }
        for (DataFlushThread flushThread : flushThreads) {
            flushThread.shutdown();
        }
        try {
            executorService.awaitTermination(consumerConfig.getConsumerShutdownTimeout(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            executorService.shutdown();
        }
    }
}
