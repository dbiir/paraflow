package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.List;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public interface DataFlow<T>
{
    Message next();

    Message[] nextBatch();

    List<Message> nextBulk();

    String getName();

    boolean isFinished();

    /**
     * map an original value to a message
     * */
    DataFlow<T> map(Function<T, Message> parser);

    DataFlow<T> keyBy(int idx);

    DataFlow<T> timeBy(int idx);

    DataFlow<T> sink(DataSink dataSink);

    DataFlow<T> partitionBy(Function<T, Integer> partitioner);
}
