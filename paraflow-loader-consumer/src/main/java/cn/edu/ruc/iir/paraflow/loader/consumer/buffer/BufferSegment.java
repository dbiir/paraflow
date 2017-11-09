package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.DynamicStringArray;

/**
 * paraflow
 *
 * @author guodong
 */
public class BufferSegment
{
    private final long segmentCapacity;
    private final int fixedFieldNum;
    private final long[] timestamps;
    private final int[] fiberIds;
    private final DynamicStringArray stringBuffer;
    private int messageNum = 0;

    public BufferSegment(long segmentCapacity, long[] timestamps, int[] fiberIds, int fixedFieldNum)
    {
        this.segmentCapacity = segmentCapacity;
        this.fixedFieldNum = fixedFieldNum;
        this.timestamps = timestamps;
        this.fiberIds = fiberIds;
        this.stringBuffer = new DynamicStringArray(fixedFieldNum);
    }

    public void addValueStride(String[] values)
    {
        // add this message value(string array) to stringBuffer
        messageNum++;
    }
}
