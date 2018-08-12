package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataFlusher
        extends Processor
{
    private final BlockingQueue<ParaflowSegment> flushingQueue;
    private final LoaderConfig config = LoaderConfig.INSTANCE();
    private final Configuration configuration;

    public DataFlusher(String name, String db, String tbl, int parallelism,
                       BlockingQueue<ParaflowSegment> flushingQueue)
    {
        super(name, db, tbl, parallelism);
        this.flushingQueue = flushingQueue;
        this.configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Override
    public void run()
    {
        try {
            while (!isReadyToStop.get()) {
                ParaflowSegment segment = flushingQueue.take();
                checkArgument(segment.getStorageLevel().equals(ParaflowSegment.StorageLevel.OFF_HEAP));
                String originPath = segment.getPath();
                String fileName = originPath.substring(originPath.lastIndexOf("/"));
                String newPath = config.getHDFSWarehouse() + "/" + segment.getDb() + "/" + segment.getTable() + "/" + fileName;
                Path outputPath = new Path(newPath);
                FileSystem fs = null;
                try {
                    fs = FileSystem.get(configuration);
                    fs.copyFromLocalFile(true, new Path(originPath), outputPath);
                    // todo update metadata
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
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
