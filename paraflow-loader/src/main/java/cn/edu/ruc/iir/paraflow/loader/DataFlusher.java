package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.BlockingQueue;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataFlusher
        extends Processor
{
    private final BlockingQueue<String> flushingQueue;
    private final MetaClient metaClient;
    private final LoaderConfig config = LoaderConfig.INSTANCE();
    private FileSystem fs = null;

    DataFlusher(String name, String db, String tbl, int parallelism,
                BlockingQueue<String> flushingQueue, MetaClient metaClient)
    {
        super(name, db, tbl, parallelism);
        this.flushingQueue = flushingQueue;
        this.metaClient = metaClient;
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
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
        try {
            while (!isReadyToStop.get()) {
                String segmentPath = flushingQueue.take();
                flushSegment(segmentPath);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void flushSegment(String segmentPath)
    {
        int fileNamePoint = segmentPath.lastIndexOf("/");
        int tblPoint = segmentPath.lastIndexOf("/", fileNamePoint - 1);
        int dbPoint = segmentPath.lastIndexOf("/", tblPoint - 1);
        String suffix = segmentPath.substring(dbPoint + 1);
        String newPath = config.getHDFSWarehouse() + suffix;
        Path outputPath = new Path(newPath);
        try {
            fs.copyFromLocalFile(true, new Path(segmentPath), outputPath);
            metaClient.updateBlockPath(segmentPath, newPath);
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

    @Override
    public void stop()
    {
        isReadyToStop.set(true);
        while (true) {
            String segmentPath = flushingQueue.poll();
            if (segmentPath == null) {
                break;
            }
            flushSegment(segmentPath);
        }
    }
}
