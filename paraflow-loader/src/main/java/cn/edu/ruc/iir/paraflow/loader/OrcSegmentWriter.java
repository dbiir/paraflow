package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class OrcSegmentWriter
        extends SegmentWriter
{
    public OrcSegmentWriter(ParaflowSegment segment, int partitionFrom, int partitionTo)
    {
        super(segment, partitionFrom, partitionTo);
    }

    @Override
    boolean write(ParaflowSegment segment, MetaProto.StringListType columnNames, MetaProto.StringListType columnTypes)
    {
        return true;
    }
}
