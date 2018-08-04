package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
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
 *                                                                                                       QueryEngine
 *                                     ConcurrentQueue             BlockingQueue                              .
 *         DataPuller(DataTransformer) ---------------> DataSorter -------                                    |
 *                        ... ...                                        |                                    |
 *         DataPuller(DataTransformer) ---------------> DataSorter -------------> DataCompactor ----. SegmentContainer ([youngZone] [adultZone])
 *                        ... ...                                        |                                    |
 *         DataPuller(DataTransformer) ---------------> DataSorter -------                                    |
 *                                                                                                            . async flushing to files (in-memory)
 *                                                                                                       SegmentWriter ---. SegmentFlusher (flushing in-memory files to the disk)
 * */
public class DefaultLoader
{
    private final ProcessPipeline pipeline;
    private final LoaderConfig config;

    public DefaultLoader()
            throws ConfigFileNotFoundException
    {
        this.pipeline = new ProcessPipeline();
        this.config = LoaderConfig.INSTANCE();
        config.init();
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
                        int pullerSorterQueueCapacity, int sorterCompactorQueueCapacity, int compactorThreshold,
                        int containerYoungZoneCapacity, int containerAdultZoneCapacity)
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
        // the blocking queue between sorters and the compactor
        BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue =
                new DisruptorBlockingQueue<>(sorterCompactorQueueCapacity, SpinPolicy.SPINNING);
        // add data pullers and sorters
        for (int pullerId : partitionMapping.keySet()) {
            ConcurrentQueue<ParaflowRecord> pullerSorterConcurrentQueue =
                    new PushPullConcurrentQueue<>(pullerSorterQueueCapacity);
            DataPuller dataPuller = new DataPuller("puller-" + pullerId, 1,
                    partitionMapping.get(pullerId), config.getProperties(), dataTransformer, pullerSorterConcurrentQueue);
            DataSorter dataSorter = new DataSorter("sorter-" + pullerId, 1, lifetime, sorterBufferCapacity,
                    pullerSorterConcurrentQueue, sorterCompactorBlockingQueue, partitionMapping.get(pullerId).size());
            pipeline.addProcessor(dataPuller);
            pipeline.addProcessor(dataSorter);
        }
        // init segment container
        SegmentContainer.INSTANCE().init(containerYoungZoneCapacity, containerAdultZoneCapacity);
        // add a data compactor
        DataCompactor dataCompactor = new DataCompactor("compactor", 1, compactorThreshold,
                topicPartitions.size(), sorterCompactorBlockingQueue);
        pipeline.addProcessor(dataCompactor);
        // start the pipeline
        pipeline.start();
    }
}
