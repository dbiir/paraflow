package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParquetWriter
        extends SegmentWriter
{
    public ParquetWriter(ParaflowSegment segment, ReadWriteLock segmentLock, SegmentCallback callback)
    {
        super(segment, segmentLock, callback);
    }

    @Override
    public void run()
    {
        super.callback.callback(segmentLock);
    }
}
