package cn.edu.ruc.iir.paraflow.commons;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class ParaflowRecord
{
    private long key;
    private long timestamp;
    private int fiberId;

    public ParaflowRecord()
    {}

    public void setKey(long key)
    {
        this.key = key;
    }

    public long getKey()
    {
        return key;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setFiberId(int fiberId)
    {
        this.fiberId = fiberId;
    }

    public int getFiberId()
    {
        return fiberId;
    }

    public abstract Object getValue(int idx);
}
