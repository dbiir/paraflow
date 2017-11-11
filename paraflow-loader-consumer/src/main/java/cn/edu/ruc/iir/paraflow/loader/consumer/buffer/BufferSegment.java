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
    private String filePath;
    private int messageNum = 0;
    private int currentIndex = 0;

    public BufferSegment(long segmentCapacity, long[] timestamps, List<TopicPartition> fiberPartitions)
    {
        this.segmentCapacity = segmentCapacity;
        this.timestamps = timestamps;
        this.fiberPartitions = fiberPartitions;
        this.stringBuffer = new DynamicStringArray();
    }

    public void addValue(String[] value)
    {
        // add this message value(string array) to stringBuffer
        stringBuffer.addValue(value);
        messageNum++;
    }

    public boolean hasNext()
    {
        return currentIndex < messageNum;
    }

    public String[] getNext()
    {
        currentIndex++;
        return stringBuffer.getValue(currentIndex);
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

    public void setFilePath(String filePath)
    {
        this.filePath = filePath;
    }

    public String getFilePath()
    {
        return filePath;
    }
}
