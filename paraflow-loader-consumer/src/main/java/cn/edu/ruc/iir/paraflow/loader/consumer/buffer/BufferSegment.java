package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.commons.TopicFiber;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.DynamicStringArray;

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
    private final List<TopicFiber> fiberPartitions;
    private final DynamicStringArray stringBuffer;
    private String filePath;
    private int messageNum = 0;
    private int currentIndex = 0;

    public BufferSegment(long segmentCapacity, long[] timestamps, List<TopicFiber> fiberPartitions)
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
        return stringBuffer.getValue(currentIndex++);
    }

    public long getSegmentCapacity()
    {
        return segmentCapacity;
    }

    public long[] getTimestamps()
    {
        return timestamps;
    }

    public List<TopicFiber> getFiberPartitions()
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
