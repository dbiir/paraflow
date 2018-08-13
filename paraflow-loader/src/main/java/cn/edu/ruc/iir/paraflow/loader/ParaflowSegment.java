package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private final ParaflowRecord[] records;   // records of each fiber
    private final long[] fiberMinTimestamps;    // minimal timestamps of each fiber
    private final long[] fiberMaxTimestamps;    // maximum timestamps of each fiber
    private final ReadWriteLock lock;
    private String db;
    private String table;
    private String path = "";                   // path of the in-memory file or on-disk file; if ON_HEAP, path is empty
    private volatile StorageLevel storageLevel;

    public ParaflowSegment(ParaflowRecord[] records, long[] fiberMinTimestamps, long[] fiberMaxTimestamps)
    {
        this.records = records;
        this.fiberMinTimestamps = fiberMinTimestamps;
        this.fiberMaxTimestamps = fiberMaxTimestamps;
        this.storageLevel = StorageLevel.ON_HEAP;
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean tryReadLock()
    {
        return this.lock.readLock().tryLock();
    }

    public void readLock()
    {
        this.lock.readLock().lock();
    }

    public void readUnLock()
    {
        this.lock.readLock().unlock();
    }

    public boolean tryWriteLock()
    {
        return this.lock.writeLock().tryLock();
    }

    public void writeLock()
    {
        this.lock.writeLock().lock();
    }

    public void writeUnLock()
    {
        this.lock.writeLock().unlock();
    }

    public ParaflowRecord[] getRecords()
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
