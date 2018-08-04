package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.locks.ReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * paraflow
 *
 * @author guodong
 */
public class SegmentFlusher
        implements Runnable
{
    private final ParaflowSegment segment;
    private final ReadWriteLock segmentLock;
    private final SegmentCallback segmentCallback;

    public SegmentFlusher(ParaflowSegment segment, ReadWriteLock segmentLock, SegmentCallback segmentCallback)
    {
        this.segment = segment;
        this.segmentLock = segmentLock;
        this.segmentCallback = segmentCallback;
    }

    @Override
    public void run()
    {
        checkArgument(segment.getStorageLevel().equals(ParaflowSegment.StorageLevel.OFF_HEAP));
        String path = segment.getPath();
        segmentCallback.callback(segmentLock);
    }
}
