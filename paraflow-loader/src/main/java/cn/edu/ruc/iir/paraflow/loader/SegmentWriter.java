package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class SegmentWriter
        implements Runnable
{
    final ParaflowSegment segment;
    final ReadWriteLock segmentLock;
    final SegmentCallback callback;

    public SegmentWriter(ParaflowSegment segment, ReadWriteLock segmentLock, SegmentCallback callback)
    {
        this.segment = segment;
        this.segmentLock = segmentLock;
        this.callback = callback;
    }

    @Override
    public abstract void run();
}
