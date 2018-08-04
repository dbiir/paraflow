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

import static com.google.common.base.Preconditions.checkArgument;

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
    private final String db;
    private final String table;
    private final int partitionFrom;
    private final int partitionTo;

    public DefaultLoader(String db, String table, int partitionFrom, int partitionTo)
            throws ConfigFileNotFoundException
    {
        this.pipeline = new ProcessPipeline();
        this.config = LoaderConfig.INSTANCE();
        config.init();
        this.db = db;
        this.table = table;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        init();
    }

    private void init()
    {
        Runtime.getRuntime().addShutdownHook(
                new Thread(pipeline::stop)
        );
    }

    public void run()
    {
        // get puller parallelism
        int pullerParallelism = Runtime.getRuntime().availableProcessors() * 2;
        if (config.contains("puller.parallelism")) {
            pullerParallelism = config.getPullerParallelism();
        }
        // get topic
        String topic = db + "-" + table;
        // assign topic partitions to each data puller
        checkArgument(partitionFrom >= partitionTo);
        int partitionNum = partitionTo - partitionFrom + 1;
        Map<Integer, List<TopicPartition>> partitionMapping = new HashMap<>();
        for (int i = partitionFrom; i <= partitionTo; i++) {
            int idx = i % pullerParallelism;
            if (!partitionMapping.containsKey(idx)) {
                partitionMapping.put(idx, new ArrayList<>());
            }
            partitionMapping.get(idx).add(new TopicPartition(topic, i));
        }
        // the blocking queue between sorters and the compactor
        BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue =
                new DisruptorBlockingQueue<>(config.getSorterCompactorCapacity(), SpinPolicy.SPINNING);
        // get transformer
        String transformerClass = config.getTransformerClass();
        DataTransformer transformer = null;
        try {
            Class clazz = DefaultLoader.class.getClassLoader().loadClass(transformerClass);
            transformer = (DataTransformer) clazz.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        if (transformer == null) {
            System.out.println("No data transformer available");
            return;
        }
        // add data pullers and sorters
        for (int pullerId : partitionMapping.keySet()) {
            ConcurrentQueue<ParaflowRecord> pullerSorterConcurrentQueue =
                    new PushPullConcurrentQueue<>(config.getPullerSorterCapacity());
            DataPuller dataPuller = new DataPuller("puller-" + pullerId, 1,
                    partitionMapping.get(pullerId), config.getProperties(), transformer, pullerSorterConcurrentQueue);
            DataSorter dataSorter = new DataSorter("sorter-" + pullerId, 1, config.getLoaderLifetime(),
                    config.getSortedBufferCapacity(), pullerSorterConcurrentQueue, sorterCompactorBlockingQueue,
                    partitionMapping.get(pullerId).size());
            pipeline.addProcessor(dataPuller);
            pipeline.addProcessor(dataSorter);
        }
        // init segment container
        SegmentContainer.INSTANCE().init(config.getContainerYoungZoneCapacity(), config.getContainerAdultZoneCapacity());
        // add a data compactor
        DataCompactor dataCompactor = new DataCompactor("compactor", 1, config.getCompactorThreshold(),
                partitionNum, sorterCompactorBlockingQueue);
        pipeline.addProcessor(dataCompactor);
        // start the pipeline
        pipeline.start();
    }
}
