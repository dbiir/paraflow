package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.DynamicStringArray;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class BufferSegment
{
    private final long segmentCapacity;
    private final long[] timestamps;
    private final List<TopicPartition> fiberPartitions;
    private final DynamicStringArray stringBuffer;
    private int messageNum = 0;

    public BufferSegment(long segmentCapacity, long[] timestamps, List<TopicPartition> fiberPartitions)
    {
        this.segmentCapacity = segmentCapacity;
        this.timestamps = timestamps;
        this.fiberPartitions = fiberPartitions;
        this.stringBuffer = new DynamicStringArray();
    }

    public void addValueStride(String[] values)
    {
        // add this message value(string array) to stringBuffer
        stringBuffer.addValue(values);
        messageNum++;
    }

    public long getSegmentCapacity()
    {
        return segmentCapacity;
    }

    public long[] getTimestamps()
    {
        return timestamps;
    }

    public List<TopicPartition> getFiberPartitions()
    {
        return fiberPartitions;
    }
}
