package cn.edu.ruc.iir.paraflow.loader.consumer.utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class DynamicStringArray
{
    private final int initSegSize;
    private final int initSegNum;
    private final int fixedValueStride;

    public DynamicStringArray(int initSegSize, int initSegNum, int fixedValueStride)
    {
        this.initSegSize = initSegSize;
        this.initSegNum = initSegNum;
        this.fixedValueStride = fixedValueStride;
    }

    public DynamicStringArray(int fixedValueStride)
    {
        this.initSegSize = 400;
        this.initSegNum = 20;
        this.fixedValueStride = fixedValueStride;
    }

    public void addValueStride(String[] values)
    {
    }
}
