package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParquetFlushThread extends DataFlushThread
{
    public ParquetFlushThread(String threadName)
    {
        super(threadName);
    }

    @Override
    boolean flushData(BufferSegment segment)
    {
        return true;
    }
}
