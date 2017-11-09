package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * paraflow
 *
 * @author guodong
 */
public class FlushQueueBuffer
{
    private long bufferSize = 0L;
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

    public BufferSegment addSegment(long segmentSize, long[] timestamps, List<TopicPartition> fiberPartitions)
    {
        if (bufferSize + segmentSize < bufferCapacity) {
            BufferSegment bufferSegment = new BufferSegment(segmentSize, timestamps, fiberPartitions);
            segments.add(bufferSegment);
            return bufferSegment;
        }
        return null;
    }

    public Optional<BufferSegment> getSegment()
    {
        BufferSegment segment = segments.poll();
        if (segment == null) {
            return Optional.empty();
        }
        bufferSize -= segment.getSegmentCapacity();
        return Optional.of(segment);
    }

    public boolean isEmpty()
    {
        return segments.isEmpty();
    }
}
