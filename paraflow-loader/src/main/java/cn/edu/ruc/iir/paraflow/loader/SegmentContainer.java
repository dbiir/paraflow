package cn.edu.ruc.iir.paraflow.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public class SegmentContainer
{
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReadWriteLock containerLock = new ReentrantReadWriteLock();
    private final AtomicInteger youngReadIndex = new AtomicInteger(0);
    private final AtomicInteger youngWriteIndex = new AtomicInteger(0);
    private final AtomicInteger adultReadIndex = new AtomicInteger(0);
    private final AtomicInteger adultWriteIndex = new AtomicInteger(0);
    private final ReadWriteLock youngZoneLock = new ReentrantReadWriteLock();
    private final ReadWriteLock adultZoneLock = new ReentrantReadWriteLock();
    private ParaflowSegment[] youngZone;    // container for on-heap segments
    private ParaflowSegment[] adultZone;    // container for off-heap segments
    private ReadWriteLock[] youngLocks;     // readWriteLock for young segments
    private ReadWriteLock[] adultLocks;     // readWriteLock for adult segments
    private int youngCapacity;
    private int adultCapacity;

    private SegmentContainer()
    {}

    private static final class SegmentContainerHolder
    {
        private static final SegmentContainer instance = new SegmentContainer();
    }

    public static SegmentContainer INSTANCE()
    {
        return SegmentContainerHolder.instance;
    }

    public synchronized void init(int youngCapacity, int adultCapacity)
    {
        if (initialized.get()) {
            return;
        }
        this.youngCapacity = youngCapacity;
        this.adultCapacity = adultCapacity;
        this.youngZone = new ParaflowSegment[youngCapacity];
        this.adultZone = new ParaflowSegment[adultCapacity];
        this.youngLocks = new ReadWriteLock[youngCapacity];
        this.adultLocks = new ReadWriteLock[adultCapacity];
        for (int i = 0; i < youngCapacity; i++) {
            youngLocks[i] = new ReentrantReadWriteLock();
        }
        for (int i = 0; i < adultCapacity; i++) {
            adultLocks[i] = new ReentrantReadWriteLock();
        }
        initialized.set(true);
    }

    public void addSegment(ParaflowSegment segment)
    {
        youngZoneLock.readLock().lock();
        if (youngWriteIndex.get() >= youngCapacity && youngReadIndex.get() < youngCapacity) {
            writeSegment(youngZone[youngReadIndex.getAndIncrement()]);
            youngWriteIndex.set(0);
            youngZone[youngWriteIndex.get()] = null;
        }
        youngZoneLock.readLock().unlock();
        youngZone[youngWriteIndex.getAndIncrement()] = segment;
        // add to container; if container is full, flush the oldest segment to a memory file, maintain segment object, but set content to null
    }

    private void writeSegment(ParaflowSegment segment)
    {
        // write to in-memory file, and set storage level and path for the segment, and move it to adultZone
    }

    private void flushSegment(ParaflowSegment segment)
    {
        // flush off-heap memory to the disk, and update metadata in MetaServer
    }

    public List<ParaflowRecord> readSegments(int[] fiberIds, long[] minTimestamps, long[] maxTimestamps)
    {
        // lock all
        // filter all segments
        // unlock all
        // lock filtered on-heap/off-heap segments
        // read content
        // unlock filtered on-heap/off-heap segments
        return new ArrayList<>();
    }
}
