package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

/**
 * paraflow
 *
 * @author guodong
 */
public class FiberFlow<T>
        extends DataFlow<T>
{
    private DataSink dataSink;
    private ParaflowFiberPartitioner partitioner;
    private MessageSerializationSchema<T> serializationSchema;
    private int keyIdx;
    private int timeIdx;

    public FiberFlow(String name, DataSource dataSource)
    {
        super(name, dataSource);
    }

    public String getName()
    {
        return super.name;
    }

    @Override
    public Message next()
    {
        return super.dataSource.read();
    }

    @Override
    public FiberFlow<T> keyBy(int idx)
    {
        this.keyIdx = idx;
        return this;
    }

    @Override
    public FiberFlow<T> timeBy(int idx)
    {
        this.timeIdx = idx;
        return this;
    }

    @Override
    public FiberFlow<T> sink(DataSink dataSink)
    {
        this.dataSink = dataSink;
        return this;
    }

    @Override
    public DataSink getSink()
    {
        return dataSink;
    }

    @Override
    public FiberFlow<T> partitionBy(ParaflowFiberPartitioner partitioner)
    {
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public FiberFlow<T> serializeBy(MessageSerializationSchema<T> serializationSchema)
    {
        this.serializationSchema = serializationSchema;
        return this;
    }

    @Override
    public MessageSerializationSchema<T> getSerializer()
    {
        return this.serializationSchema;
    }

    @Override
    public ParaflowFiberPartitioner getPartitioner()
    {
        return this.partitioner;
    }
}
