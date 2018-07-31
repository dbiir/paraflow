package cn.edu.ruc.iir.paraflow.loader.buffer;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * paraflow
 *
 * @author guodong
 */
public class FlushQueueBuffer
{
    private AtomicLong bufferSize = new AtomicLong(0L);
    private long bufferCapacity = 0L;

    private final ConcurrentLinkedQueue<BufferSegment> segments;

    private FlushQueueBuffer()
    {
        this.segments = new ConcurrentLinkedQueue<>();
    }

    private static class FlushBufferHolder
    {
        private static final FlushQueueBuffer instance = new FlushQueueBuffer();
    }

    public static final FlushQueueBuffer INSTANCE()
    {
        return FlushBufferHolder.instance;
    }

    void setBufferCapacity(long bufferCapacity)
    {
        this.bufferCapacity = bufferCapacity;
    }

    public boolean addSegment(long segmentSize, BufferSegment segment)
    {
        if (bufferSize.get() < bufferCapacity - segmentSize) {
//            BufferSegment bufferSegment = new BufferSegment(segmentSize, timestamps, fiberPartitions);
            segments.add(segment);
            bufferSize.addAndGet(segmentSize);
            return true;
        }
        return false;
    }

    public Optional<BufferSegment> getSegment()
    {
        BufferSegment segment = segments.poll();
        if (segment == null) {
            return Optional.empty();
        }
        bufferSize.addAndGet(-segment.getSegmentCapacity());
        return Optional.of(segment);
    }

    public boolean isEmpty()
    {
        return segments.isEmpty();
    }
}
