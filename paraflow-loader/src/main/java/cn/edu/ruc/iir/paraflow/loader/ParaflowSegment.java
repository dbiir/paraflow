package cn.edu.ruc.iir.paraflow.loader;

/**
 * paraflow segment
 *
 * @author guodong
 */
public class ParaflowSegment
{
    public enum StorageLevel {
        ON_HEAP, OFF_HEAP, ON_DISK
    }

    private final ParaflowRecord[][] records;   // records of each fiber
    private final long[] fiberMinTimestamps;    // minimal timestamps of each fiber
    private final long[] fiberMaxTimestamps;    // maximum timestamps of each fiber
    private String db;
    private String table;
    private String path = "";                   // path of the in-memory file or on-disk file; if ON_HEAP, path is empty
    private StorageLevel storageLevel;

    public ParaflowSegment(ParaflowRecord[][] records)
    {
        this.records = records;
        this.fiberMinTimestamps = new long[records.length];
        this.fiberMaxTimestamps = new long[records.length];
        for (int i = 0; i < records.length; i++) {
            fiberMinTimestamps[i] = records[i][0].getTimestamp();
            fiberMaxTimestamps[i] = records[i][records.length - 1].getTimestamp();
        }
        this.storageLevel = StorageLevel.ON_HEAP;
    }

    public ParaflowRecord[][] getRecords()
    {
        return records;
    }

    public void setDb(String db)
    {
        this.db = db;
    }

    public String getDb()
    {
        return db;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public String getTable()
    {
        return table;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getPath()
    {
        return path;
    }

    public void setStorageLevel(StorageLevel storageLevel)
    {
        this.storageLevel = storageLevel;
    }

    public StorageLevel getStorageLevel()
    {
        return storageLevel;
    }

    public long[] getFiberMaxTimestamps()
    {
        return fiberMaxTimestamps;
    }

    public long[] getFiberMinTimestamps()
    {
        return fiberMinTimestamps;
    }
}
