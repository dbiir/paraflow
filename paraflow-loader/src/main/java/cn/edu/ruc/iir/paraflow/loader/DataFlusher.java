package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.Metric;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.BlockingQueue;

public class DataFlusher
        extends Processor
{
    private final Logger logger = LoggerFactory.getLogger(DataFlusher.class);
    private final BlockingQueue<ParaflowSegment> flushingQueue;
    private final MetaClient metaClient;
    private final Metric metric;
    private final boolean metricEnabled;
    private final int partitionFrom;
    private final LoaderConfig config = LoaderConfig.INSTANCE();
    private FileSystem fs = null;

    DataFlusher(String name, String db, String tbl, int parallelism, int partitionFrom,
                BlockingQueue<ParaflowSegment> flushingQueue, MetaClient metaClient)
    {
        super(name, db, tbl, parallelism);
        this.partitionFrom = partitionFrom;
        this.flushingQueue = flushingQueue;
        this.metaClient = metaClient;
        this.metric = new Metric(config.getGateWayUrl(), config.getLoaderId(), "loader_latency", "Loader latency (ms)", "paraflow_loader");
        this.metricEnabled = config.isMetricEnabled();
        Configuration configuration = new Configuration(false);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("dfs.replication", "1");
        System.out.println("DFS replication " + configuration.get("dfs.replication"));
        logger.info("DFS replication " + configuration.get("dfs.replication"));
        try {
            fs = FileSystem.get(URI.create(config.getHDFSWarehouse()), configuration);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        System.out.println(super.name + " started.");
        try {
            while (!isReadyToStop.get()) {
                ParaflowSegment segment = flushingQueue.take();
                flushSegment(segment);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop()
    {
        isReadyToStop.set(true);
        while (true) {
            ParaflowSegment segment = flushingQueue.poll();
            if (segment == null) {
                break;
            }
            flushSegment(segment);
        }
    }

    private void flushSegment(ParaflowSegment segment)
    {
        String segmentPath = segment.getPath();
        int fileNamePoint = segmentPath.lastIndexOf("/");
        int tblPoint = segmentPath.lastIndexOf("/", fileNamePoint - 1);
        int dbPoint = segmentPath.lastIndexOf("/", tblPoint - 1);
        String suffix = segmentPath.substring(dbPoint + 1);
        String newPath = config.getHDFSWarehouse() + suffix;
        Path outputPath = new Path(newPath);
        try {
            fs.create(outputPath, (short) 1);
            fs.copyFromLocalFile(true, new Path(segmentPath), outputPath);
            // add block index
            long[] fiberMinTimestamps = segment.getFiberMinTimestamps();
            long[] fiberMaxTimestamps = segment.getFiberMaxTimestamps();
            long writeTime = segment.getWriteTime();
            int partitionNum = fiberMinTimestamps.length;
            for (int i = 0; i < partitionNum; i++) {
                if (fiberMinTimestamps[i] == -1) {
                    continue;
                }
                if (fiberMaxTimestamps[i] == -1) {
                    continue;
                }
                metaClient.createBlockIndex(db, table, i + partitionFrom, fiberMinTimestamps[i], fiberMaxTimestamps[i], newPath);
            }
            double writeLatency = Math.abs(writeTime - segment.getAvgTimestamp());
//            double flushLatency = Math.abs(System.currentTimeMillis() - segment.getAvgTimestamp());
            logger.info("write latency: " + writeLatency + " ms.");
//            logger.info("flush latency: " + flushLatency + " ms.");
            if (metricEnabled) {
                metric.addValue(writeLatency);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            if (fs != null) {
                try {
                    fs.deleteOnExit(outputPath);
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}
