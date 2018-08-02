package cn.edu.ruc.iir.paraflow.loader;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowRecord
{
    private Object key;
    private long timestamp;
    private long fiberId;
    private Object[] values;

//    public ParaflowRecord(Object key, long timestamp, long fiberId, Object... values)
//    {
//        this.key = key;
//        this.timestamp = timestamp;
//        this.fiberId = fiberId;
//        this.values = values;
//    }

    public Object getKey()
    {
        return key;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public long getFiberId()
    {
        return fiberId;
    }

    public Object[] getValues()
    {
        return values;
    }

    public Object getValue(int idx)
    {
        return values[idx];
    }
}
