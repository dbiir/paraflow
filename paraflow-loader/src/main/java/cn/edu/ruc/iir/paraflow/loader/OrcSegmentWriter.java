package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public class OrcSegmentWriter
        extends SegmentWriter
{
    public OrcSegmentWriter(ParaflowSegment segment, ReadWriteLock segmentLock, SegmentCallback callback,
                            int partitionFrom, int partitionTo)
    {
        super(segment, segmentLock, callback, partitionFrom, partitionTo);
    }

    @Override
    boolean write(ParaflowSegment segment, MetaProto.StringListType columnNames, MetaProto.StringListType columnTypes)
    {
        return true;
    }
}
