package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.LogManager;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class SegmentWriter
        implements Runnable
{
    private final ParaflowSegment segment;
    private final int partitionFrom;
    private final int partitionTo;
    private final Random random = new Random(System.currentTimeMillis());
    private final MetaClient metaClient;
    private final Map<String, MetaProto.StringListType> tableColumnNamesCache;
    private final Map<String, MetaProto.StringListType> tableColumnTypesCache;
    final LoaderConfig config = LoaderConfig.INSTANCE();
    final Configuration configuration = new Configuration();

    SegmentWriter(ParaflowSegment segment, int partitionFrom, int partitionTo, MetaClient metaClient)
    {
        this.segment = segment;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.metaClient = metaClient;
        this.tableColumnNamesCache = new HashMap<>();
        this.tableColumnTypesCache = new HashMap<>();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Override
    public void run()
    {
        LogManager.getLogManager().reset();
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
            // change storage level
            segment.setStorageLevel(ParaflowSegment.StorageLevel.OFF_HEAP);
            // update metadata
            long[] fiberMinTimestamps = segment.getFiberMinTimestamps();
            long[] fiberMaxTimestamps = segment.getFiberMaxTimestamps();
            int partitionNum = partitionTo - partitionFrom + 1;
            for (int i = 0; i < partitionNum; i++) {
                if (fiberMinTimestamps[i] == -1) {
                    continue;
                }
                if (fiberMaxTimestamps[i] == -1) {
                    continue;
                }
                metaClient.createBlockIndex(db, table, i + partitionFrom, fiberMinTimestamps[i], fiberMaxTimestamps[i], path);
            }
        }
    }

    abstract boolean write(ParaflowSegment segment, MetaProto.StringListType columnNames, MetaProto.StringListType columnTypes);
}
