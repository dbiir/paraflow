package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
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
 * <p>
 * default pipeline:
 * QueryEngine
 * ConcurrentQueue             BlockingQueue                              .
 * DataPuller(DataTransformer) ---------------> DataSorter -------                                    |
 * ... ...                                        |                                    |
 * DataPuller(DataTransformer) ---------------> DataSorter -------------> DataCompactor ----. SegmentContainer ([youngZone] [adultZone])
 * ... ...                                        |                                    |
 * DataPuller(DataTransformer) ---------------> DataSorter -------                                    |
 * . async flushing to files (in-memory)
 * SegmentWriter ---. DataFlusher (flushing in-memory files to the disk)
 */
public class DefaultLoader
{
    private final ProcessPipeline pipeline;
    private final LoaderConfig config;
    private final String db;
    private final String table;
    private final int partitionFrom;
    private final int partitionTo;
    private final MetaClient metaClient;

    public DefaultLoader(String db, String table, int partitionFrom, int partitionTo)
            throws ConfigFileNotFoundException
    {
        this.config = LoaderConfig.INSTANCE();
        config.init();
        this.pipeline = new ProcessPipeline(config);
        this.db = db;
        this.table = table;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.metaClient = new MetaClient(config.getMetaServerHost(), config.getMetaServerPort());
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
        int pullerParallelism = Runtime.getRuntime().availableProcessors();
        if (config.contains("puller.parallelism")) {
            pullerParallelism = config.getPullerParallelism();
        }
        // get topic
        String topic = db + "-" + table;
        // assign topic partitions to each data puller
        checkArgument(partitionTo >= partitionFrom);
        int partitionNum = partitionTo - partitionFrom + 1;
        Map<Integer, List<TopicPartition>> partitionMapping = new HashMap<>();
        for (int i = partitionFrom; i <= partitionTo; i++) {
            int idx = i % pullerParallelism;
            if (!partitionMapping.containsKey(idx)) {
                partitionMapping.put(idx, new ArrayList<>((partitionNum / pullerParallelism) + 2));
            }
            partitionMapping.get(idx).add(new TopicPartition(topic, i));
        }
        // the blocking queue between sorters and the compactor
        BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue =
                new DisruptorBlockingQueue<>(config.getSorterCompactorCapacity(), SpinPolicy.SPINNING);
        // get transformer
        String transformerClass = config.getTransformerClass();

        // add data pullers and sorters
        for (int pullerId : partitionMapping.keySet()) {
            DataPuller dataPuller = new DataPuller("puller-" + pullerId, db, table, 1,
                    partitionMapping.get(pullerId), config.getProperties(), transformerClass,
                    sorterCompactorBlockingQueue, config.getSortedBufferCapacity());
            pipeline.addProcessor(dataPuller);
        }
        // init segment container
        BlockingQueue<ParaflowSegment> flushingQueue =
                new PushPullBlockingQueue<>(config.getFlushingCapacity(), SpinPolicy.SPINNING);
        SegmentContainer.INSTANCE().init(config.getContainerCapacity(), partitionFrom, partitionTo,
                flushingQueue, pipeline.getExecutorService(), metaClient);
        // add a data compactor
        DataCompactor dataCompactor = new DataCompactor("compactor", db, table, 1, config.getCompactorThreshold(),
                partitionFrom, partitionNum, sorterCompactorBlockingQueue);
        pipeline.addProcessor(dataCompactor);
        // add a data flusher
        DataFlusher dataFlusher = new DataFlusher("flusher", db, table, 1, partitionFrom, flushingQueue, metaClient);
        pipeline.addProcessor(dataFlusher);
        // start the pipeline
        pipeline.start();
    }
}
