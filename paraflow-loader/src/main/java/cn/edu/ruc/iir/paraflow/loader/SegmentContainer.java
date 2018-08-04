package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.atomic.AtomicBoolean;
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
    private int youngReadIndex = 0;
    private int youngWriteIndex = 0;
    private int adultReadIndex = 0;
    private int adultWriteIndex = 0;
    private ParaflowSegment[] youngZone;    // container for on-heap segments
    private ParaflowSegment[] adultZone;    // container for off-heap segments
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
        this.adultLocks = new ReadWriteLock[adultCapacity];
        for (int i = 0; i < adultCapacity; i++) {
            adultLocks[i] = new ReentrantReadWriteLock();
        }
        initialized.set(true);
    }

    public void addSegment(ParaflowSegment segment)
    {
        if (youngReadIndex == youngWriteIndex && youngReadIndex != 0) {
            growUp(youngZone[youngReadIndex]);
            youngReadIndex++;
        }
        youngZone[youngWriteIndex++] = segment;
        if (youngWriteIndex >= youngCapacity) {
            youngWriteIndex = 0;
        }
    }

    private void growUp(ParaflowSegment segment)
    {
        // if no space left, flush out one segment to disk
        if (adultReadIndex == adultWriteIndex && adultReadIndex != 0) {
            try {
                while (!adultLocks[adultReadIndex].readLock().tryLock()) {
                    Thread.sleep(100);
                }
                new Thread(new SegmentFlusher(segment, adultLocks[adultReadIndex], lock -> {
                    lock.readLock().unlock();
                })).start();
                adultReadIndex++;
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // write the segment
        try {
            // wait until the segment at readIndex is already flushed to disk
            while (!adultLocks[adultWriteIndex].writeLock().tryLock()) {
                Thread.sleep(1000);
            }
            new Thread(new ParquetWriter(segment, adultLocks[adultWriteIndex], lock -> {
                adultZone[adultWriteIndex] = segment;
                lock.writeLock().unlock();
            })).start();
            adultWriteIndex++;
            if (adultWriteIndex >= adultCapacity) {
                adultWriteIndex = 0;
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String readOnHeapSegment(int index)
    {
        adultLocks[index].readLock().lock();
        return adultZone[index].getPath();
    }

    public void doneRead(int index)
    {
        adultLocks[index].readLock().unlock();
    }
}
