package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;

/**
 * paraflow
 *
 * @author guodong
 */
public class PlainTextFlushThread extends DataFlushThread
{
    @Override
    boolean flushData(BufferSegment segment)
    {
        return false;
    }
}
