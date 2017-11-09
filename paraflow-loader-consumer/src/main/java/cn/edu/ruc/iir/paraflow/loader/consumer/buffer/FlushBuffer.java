package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import java.util.LinkedList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class FlushBuffer
{
    private long bufferSize = 0L;
    private long bufferCapacity = 0L;
    private int fixedFieldNum = 0;

    private final List<BufferSegment> segments;

    private FlushBuffer()
    {
        this.segments = new LinkedList<>();
    }

    private static class FlushBufferHolder
    {
        private static final FlushBuffer instance = new FlushBuffer();
    }

    public static final FlushBuffer INSTANCE()
    {
        return FlushBufferHolder.instance;
    }

    void setBufferCapacity(long bufferCapacity)
    {
        this.bufferCapacity = bufferCapacity;
    }

    void setFixedFieldSize(int fixedFieldNum)
    {
        this.fixedFieldNum = fixedFieldNum;
    }

    public BufferSegment newSegment(long segmentSize, long[] timestamps, int[] fiberIds)
    {
        if (bufferSize + segmentSize < bufferCapacity) {
            BufferSegment bufferSegment = new BufferSegment(segmentSize, timestamps, fiberIds, fixedFieldNum);
            segments.add(bufferSegment);
        }
        return null;
    }
}
