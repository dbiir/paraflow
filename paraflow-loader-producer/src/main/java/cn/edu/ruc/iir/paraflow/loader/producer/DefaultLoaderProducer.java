package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultLoaderProducer implements LoaderProducer
{
    @Override
    public int send(String database, String table, Message message)
    {
        return 0;
    }

    @Override
    public void setTblTopicMapping(String database, String table, String host, String topic)
    {
    }

    @Override
    public void createDatabase()
    {
    }

    @Override
    public void createTable()
    {
    }

    @Override
    public void createFiberFunc(String funcName, Function<Long, Integer> func)
    {
    }

    @Override
    public void registerFilter(String database, String table, Function<Message, Boolean> filterFunc)
    {
    }
}
