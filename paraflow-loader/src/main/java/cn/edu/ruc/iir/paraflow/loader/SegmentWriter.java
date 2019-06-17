package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

public abstract class SegmentWriter
        implements Runnable
{
    final LoaderConfig config = LoaderConfig.INSTANCE();
    final Configuration configuration = new Configuration();
    private final Logger logger = LoggerFactory.getLogger(SegmentWriter.class);
    private final ParaflowSegment segment;
    private final Random random = new Random(System.nanoTime());
    private final MetaClient metaClient;
    private final BlockingQueue<ParaflowSegment> flushingQueue;
    private final Map<String, MetaProto.StringListType> tableColumnNamesCache;
    private final Map<String, MetaProto.StringListType> tableColumnTypesCache;

    SegmentWriter(ParaflowSegment segment, MetaClient metaClient,
                  BlockingQueue<ParaflowSegment> flushingQueue)
    {
        this.segment = segment;
        this.metaClient = metaClient;
        this.flushingQueue = flushingQueue;
        this.tableColumnNamesCache = new HashMap<>();
        this.tableColumnTypesCache = new HashMap<>();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Override
    public void run()
    {
        // generate file path
        String db = segment.getDb();
        String table = segment.getTable();
        String path = config.getMemoryWarehouse() + db + "/" + table + "/"
                + config.getLoaderId() + System.nanoTime() + random.nextInt();
        segment.setPath(path);
        // get metadata
        String key = db + "-" + table;
        MetaProto.StringListType columnNames;
        MetaProto.StringListType columnTypes;
        if (tableColumnNamesCache.containsKey(key)) {
            columnNames = tableColumnNamesCache.get(key);
        }
        else {
            columnNames = metaClient.listColumns(db, table);
            tableColumnNamesCache.put(key, columnNames);
        }
        if (tableColumnTypesCache.containsKey(key)) {
            columnTypes = tableColumnTypesCache.get(key);
        }
        else {
            columnTypes = metaClient.listColumnsDataType(db, table);
            tableColumnTypesCache.put(key, columnTypes);
        }
        // write file
        if (write(segment, columnNames, columnTypes)) {
            // signal done segment
            SegmentContainer.INSTANCE().doneSegment();
            // change storage level
            segment.setStorageLevel(ParaflowSegment.StorageLevel.OFF_HEAP);
            segment.setWriteTime(System.currentTimeMillis());
            logger.debug("Done writing a segment " + segment.getPath());
            // clear segment content
            segment.clearRecords();
            // flush segment
            try {
                flushingQueue.put(segment);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    abstract boolean write(ParaflowSegment segment, MetaProto.StringListType columnNames, MetaProto.StringListType columnTypes);
}
