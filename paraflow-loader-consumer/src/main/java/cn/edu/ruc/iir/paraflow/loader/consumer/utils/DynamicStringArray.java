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

    public DynamicStringArray(int initSegSize, int initSegNum)
    {
        this.initSegSize = initSegSize;
        this.initSegNum = initSegNum;
    }

    public DynamicStringArray()
    {
        this.initSegSize = 400;
        this.initSegNum = 20;
    }

    public void addValueStride(String[] values)
    {
    }
}
