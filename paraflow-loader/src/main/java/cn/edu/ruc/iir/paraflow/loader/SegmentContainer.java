package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class SegmentContainer
{
    private final Logger logger = LoggerFactory.getLogger(SegmentContainer.class);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicInteger containerSize = new AtomicInteger(0);

    private ExecutorService executorService;
    private MetaClient metaClient;
    private int capacity;
    private int partitionFrom;
    private int partitionTo;
    private BlockingQueue<ParaflowSegment> flushingQueue;

    private SegmentContainer()
    {
    }

    static SegmentContainer INSTANCE()
    {
        return SegmentContainerHolder.instance;
    }

    synchronized void init(int capacity, int partitionFrom, int partitionTo,
                           BlockingQueue<ParaflowSegment> flushingQueue, ExecutorService executorService, MetaClient metaClient)
    {
        if (initialized.get()) {
            return;
        }
        this.capacity = capacity;
        this.partitionFrom = partitionFrom;
        this.partitionTo = partitionTo;
        this.flushingQueue = flushingQueue;
        this.executorService = executorService;
        this.metaClient = metaClient;
        initialized.set(true);
    }

    /**
     * if (container.size >= capacity) {
     * loop over current container to find a segment that is OFF_HEAP
     * if find, send it to the FlushingBlockingQueue, and set it as current segment, return true;
     * if not find, which means all segments are busy, return false;
     * }
     * else {
     * container[writeIndex++] = segment;
     * start a writer thread for the segment;
     * size++;
     * return true;
     * }
     */
    boolean addSegment(ParaflowSegment segment)
    {
        if (containerSize.get() >= capacity) {
            return false;
        }
        else {
            int currentSize = containerSize.incrementAndGet();
            logger.debug("current container size: " + currentSize);
            executorService.execute(
                    new Thread(new ParquetSegmentWriter(segment, metaClient, flushingQueue)));
            return true;
        }
    }

    void doneSegment()
    {
        containerSize.decrementAndGet();
    }

    /**
     * Call this when the process stops, and flushes all off-heap segments to disks
     */
    void stop()
    {
    }

    private static final class SegmentContainerHolder
    {
        private static final SegmentContainer instance = new SegmentContainer();
    }
}
