package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;

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

    private ParaflowRecord[][] records;   // records of each fiber
    private long[] fiberMinTimestamps;    // minimal timestamp of each fiber
    private long[] fiberMaxTimestamps;    // maximum timestamp of each fiber
    private double avgTimestamp;          // average timestamp of all fibers
    private String db;
    private String table;
    private String path = "";             // path of the in-memory file or on-disk file; if ON_HEAP, path is empty
    private long writeTime;
    private long flushTime;
    private volatile StorageLevel storageLevel;

    public ParaflowSegment(ParaflowRecord[][] records, long[] fiberMinTimestamps, long[] fiberMaxTimestamps,
                           double avgTimestamp)
    {
        this.records = records;
        this.fiberMinTimestamps = fiberMinTimestamps;
        this.fiberMaxTimestamps = fiberMaxTimestamps;
        this.avgTimestamp = avgTimestamp;
        this.storageLevel = StorageLevel.ON_HEAP;
    }

    public void clearRecords()
    {
        this.records = null;
    }

    public void clearTimestamps()
    {
        this.fiberMinTimestamps = null;
        this.fiberMaxTimestamps = null;
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

    public double getAvgTimestamp()
    {
        return avgTimestamp;
    }

    public void setWriteTime(long writeTime)
    {
        this.writeTime = writeTime;
    }

    public void setFlushTime(long flushTime)
    {
        this.flushTime = flushTime;
    }

    public long getWriteTime()
    {
        return writeTime;
    }

    public long getFlushTime()
    {
        return flushTime;
    }
}
