package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import com.conversantmedia.util.concurrent.ConcurrentQueue;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.PushPullConcurrentQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * paraflow default loader
 *
 * default pipeline:
 *                                     ConcurrentQueue             BlockingQueue
 *         DataPuller(DataTransformer) ---------------> DataSorter -------
 *                        ... ...                                        |
 *         DataPuller(DataTransformer) ---------------> DataSorter -------------> DataCompactor
 *                        ... ...                                        |              |
 *         DataPuller(DataTransformer) ---------------> DataSorter -------              |
 *                                                                                      .          Future
 *                                                                                SegmentContainer ------> DataFlusher
 *                                                                                      |           async
 *                                                                                      |
 *                                                                                      .
 *                                                                                 QueryEngine
 * */
public class DefaultLoader
{
    private final ProcessPipeline pipeline;
    private final ConsumerConfig config;

    public DefaultLoader(String configPath)
            throws ConfigFileNotFoundException
    {
        this.pipeline = new ProcessPipeline();
        this.config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        init();
    }

    private void init()
    {
        Runtime.getRuntime().addShutdownHook(
                new Thread(pipeline::stop)
        );
    }

    public void consume(List<TopicPartition> topicPartitions, DataTransformer dataTransformer,
                        int parallelism, long lifetime, int sorterBufferCapacity,
                        int pullerSorterQueueCapacity, int sorterCompactorQueueCapacity)
    {
        // assign topic partitions to each data puller
        Map<Integer, List<TopicPartition>> partitionMapping = new HashMap<>();
        for (int i = 0; i < topicPartitions.size(); i++) {
            int idx = i % parallelism;
            if (!partitionMapping.containsKey(idx)) {
                partitionMapping.put(idx, new ArrayList<>());
            }
            partitionMapping.get(idx).add(topicPartitions.get(i));
        }
        // add data pullers and sorters
        for (int pullerId : partitionMapping.keySet()) {
            ConcurrentQueue<ParaflowRecord> pullerSorterConcurrentQueue = new PushPullConcurrentQueue<>(pullerSorterQueueCapacity);
            DataPuller dataPuller = new DataPuller("puller-" + pullerId, 1,
                    partitionMapping.get(pullerId), config.getProperties(), dataTransformer, pullerSorterConcurrentQueue);
            DataSorter dataSorter = new DataSorter("sorter-" + pullerId, 1, lifetime, sorterBufferCapacity,
                    pullerSorterConcurrentQueue, partitionMapping.get(pullerId).size());
            pipeline.addProcessor(dataPuller);
            pipeline.addProcessor(dataSorter);
        }
        // add a data compactor
        BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue =
                new DisruptorBlockingQueue<>(sorterCompactorQueueCapacity, SpinPolicy.SPINNING);
        // start the pipeline
        pipeline.start();
    }
}
