package cn.edu.ruc.iir.paraflow.loader;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
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
                           BlockingQueue<ParaflowSegment> flushingQueue)
    {
        if (initialized.get()) {
            return;
        }
        this.containerCapacity = containerCapacity;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.flushingQueue = flushingQueue;
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
            // todo use our own executor service to submit writer tasks
            new Thread(new ParquetSegmentWriter(segment, partitionFrom, partitionTo)).start();
            return true;
        }
        return false;
    }
}
