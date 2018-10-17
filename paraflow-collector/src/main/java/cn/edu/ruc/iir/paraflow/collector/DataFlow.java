package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * dataflow interface
 *
 * @author guodong
 */
public abstract class DataFlow<T>
{
    protected final String name;
    protected final DataSource dataSource;
    protected AtomicBoolean finished = new AtomicBoolean(false);

    public DataFlow(String name, DataSource dataSource)
    {
        this.name = name;
        this.dataSource = dataSource;
    }

    public String getName()
    {
        return this.name;
    }

    public void setFinished()
    {
        this.finished.set(true);
    }

    public boolean isFinished()
    {
        return this.finished.get();
    }

    public abstract Message next();

    public abstract FiberFlow<T> keyBy(int idx);

    public abstract FiberFlow<T> timeBy(int idx);

    public abstract FiberFlow<T> sink(DataSink dataSink);

    public abstract DataSink getSink();

    public abstract FiberFlow<T> partitionBy(ParaflowFiberPartitioner partitioner);

    public abstract ParaflowFiberPartitioner getPartitioner();
}
