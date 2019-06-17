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
    {
    }

    public long getKey()
    {
        return key;
    }

    public void setKey(long key)
    {
        this.key = key;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public int getFiberId()
    {
        return fiberId;
    }

    public void setFiberId(int fiberId)
    {
        this.fiberId = fiberId;
    }

    public abstract Object getValue(int idx);
}
