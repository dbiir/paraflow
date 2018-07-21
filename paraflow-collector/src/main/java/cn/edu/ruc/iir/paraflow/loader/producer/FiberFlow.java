package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * paraflow
 *
 * @author guodong
 */
public class FiberFlow<T>
        implements DataFlow<T>
{
    private final DataSource<T> dataSource;
    private DataSink dataSink;
    private Function<T, Message> parser;
    private Function<T, Integer> partitioner;
    private int keyIdx;
    private int timeIdx;
    private boolean finished = false;

    public FiberFlow(DataSource<T> dataSource)
    {
        this.dataSource = dataSource;
    }

    public String getName()
    {
        return dataSource.getName();
    }

    @Override
    public Message next()
    {
        return parser.apply(dataSource.read());
    }

    @Override
    public Message[] nextBatch()
    {
        return nextBulk().toArray(new Message[0]);
    }

    @Override
    public List<Message> nextBulk()
    {
        return dataSource.readBulk().stream().map(parser).collect(Collectors.toList());
    }

    /**
     * map an original value to a message
     *
     * @param parser
     */
    @Override
    public FiberFlow<T> map(Function<T, Message> parser)
    {
        this.parser = parser;
        return this;
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
        return null;
    }

    @Override
    public FiberFlow<T> sink(DataSink dataSink)
    {
        this.dataSink = dataSink;
        return this;
    }

    @Override
    public FiberFlow<T> partitionBy(Function<T, Integer> partitioner)
    {
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
