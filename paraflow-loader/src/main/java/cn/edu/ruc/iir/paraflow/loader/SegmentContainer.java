package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * paraflow
 *
 * @author guodong
 */
class SegmentContainer
{
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final List<ParaflowSegment> container;

    private int containerSize = 0;
    private BlockingQueue<ParaflowSegment> flushingQueue;
    private ExecutorService executorService;
    private MetaClient metaClient;
    private int containerCapacity;
    private int partitionFrom;
    private int partitionTo;

    private SegmentContainer()
    {
        this.container = new LinkedList<>();
    }

    private static final class SegmentContainerHolder
    {
        private static final SegmentContainer instance = new SegmentContainer();
    }

    static SegmentContainer INSTANCE()
    {
        return SegmentContainerHolder.instance;
    }

    synchronized void init(int containerCapacity, int partitionFrom, int partitionTo,
                           BlockingQueue<ParaflowSegment> flushingQueue, ExecutorService executorService, MetaClient metaClient)
    {
        if (initialized.get()) {
            return;
        }
        this.containerCapacity = containerCapacity;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.flushingQueue = flushingQueue;
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
        if (containerSize >= containerCapacity) {
            for (ParaflowSegment sg : container) {
                if (sg.getStorageLevel() == ParaflowSegment.StorageLevel.OFF_HEAP) {
                    if (flushingQueue.add(sg)) {
                        container.remove(sg);
                        containerSize--;
                    }
                }
            }
        }
        else {
            container.add(segment);
            containerSize++;
            executorService.execute(
                    new Thread(new ParquetSegmentWriter(segment, partitionFrom, partitionTo, metaClient)));
            return true;
        }
        return false;
    }

    /**
     * Call this when the process stops, and flushes all off-heap segments to disks
     * */
    void flushAll()
    {
        for (ParaflowSegment sg : container) {
            if (sg.getStorageLevel() == ParaflowSegment.StorageLevel.OFF_HEAP) {
                flushingQueue.add(sg);
            }
        }
    }
}
