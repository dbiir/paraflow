package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * paraflow
 *
 * @author guodong
 */
class SegmentContainer
{
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private YoungZone youngZone = null;
    private AdultZone adultZone = null;

    private ExecutorService executorService;
    private MetaClient metaClient;
    private int partitionFrom;
    private int partitionTo;

    private SegmentContainer()
    {
    }

    private static final class SegmentContainerHolder
    {
        private static final SegmentContainer instance = new SegmentContainer();
    }

    static SegmentContainer INSTANCE()
    {
        return SegmentContainerHolder.instance;
    }

    synchronized void init(int youngCapacity, int adultCapacity, int partitionFrom, int partitionTo,
                           BlockingQueue<String> flushingQueue, ExecutorService executorService, MetaClient metaClient)
    {
        if (initialized.get()) {
            return;
        }
        this.youngZone = new YoungZone(youngCapacity);
        this.adultZone = new AdultZone(adultCapacity, flushingQueue);
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.executorService = executorService;
        this.metaClient = metaClient;
        initialized.set(true);
    }

    /**
     * if (container.size >= capacity) {
     *     loop over current container to find a segment that is OFF_HEAP
     *     if find, send it to the FlushingBlockingQueue, and set it as current segment, return true;
     *     if not find, which means all segments are busy, return false;
     * }
     * else {
     *     container[writeIndex++] = segment;
     *     start a writer thread for the segment;
     *     size++;
     *     return true;
     * }
     * */
    boolean addSegment(ParaflowSegment segment)
    {
//        if (containerSize >= containerCapacity) {
//            for (ParaflowSegment sg : container) {
//                if (sg.getStorageLevel() == ParaflowSegment.StorageLevel.OFF_HEAP) {
//                    if (flushingQueue.add(sg.getPath())) {
//                        container.remove(sg);
//                        containerSize--;
//                    }
//                }
//            }
//        }
//        else {
//            container.add(segment);
//            containerSize++;
//            return true;
//        }
//        return false;
        return youngZone.addSegment(segment);
    }

    void growUp(String segment)
    {
        adultZone.addSegment(segment);
    }

    /**
     * Call this when the process stops, and flushes all off-heap segments to disks
     * */
    void stop()
    {
        while (!youngZone.isEmpty()) {
            // wait, do nothing
        }
        adultZone.flushAll();
    }

    private class YoungZone
    {
        private final AtomicInteger counter;
        private final int capacity;

        YoungZone(int capacity)
        {
            this.counter = new AtomicInteger(0);
            this.capacity = capacity;
        }

        boolean addSegment(ParaflowSegment segment)
        {
            if (counter.get() < capacity) {
                counter.incrementAndGet();
                executorService.execute(
                        new Thread(new ParquetSegmentWriter(segment, partitionFrom, partitionTo, metaClient, counter)));
                return true;
            }
            return false;
        }

        boolean isEmpty()
        {
            return counter.get() == 0;
        }
    }

    private class AdultZone
    {
        private final Queue<String> adultZoneQueue;
        private final int capacity;
        private final BlockingQueue<String> flushingQueue;

        AdultZone(int capacity, BlockingQueue<String> flushingQueue)
        {
            this.adultZoneQueue = new LinkedList<>();
            this.capacity = capacity;
            this.flushingQueue = flushingQueue;
        }

        void addSegment(String segment)
        {
            if (adultZoneQueue.size() < capacity) {
                adultZoneQueue.add(segment);
            }
            else {
                String flushingSegment = adultZoneQueue.poll();
                if (flushingSegment == null) {
                    adultZoneQueue.add(segment);
                    return;
                }
                while (!flushingQueue.add(flushingSegment)) {
                    // wait, do nothing
                }
                adultZoneQueue.add(segment);
            }
        }

        void flushAll()
        {
            while (!adultZoneQueue.isEmpty()) {
                String flushingSegment = adultZoneQueue.poll();
                if (flushingSegment == null) {
                    return;
                }
                while (!flushingQueue.add(flushingSegment)) {
                    // wait, do nothing
                }
            }
        }
    }
}
