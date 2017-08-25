package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public interface LoaderProducer
{
    int send(String database, String table, Message message);

    void setTblTopicMapping(String database, String table, String host, String topic);

    void createDatabase();

    void createTable();

    void createFiberFunc(String funcName, Function<Long, Integer> func);

    void registerFilter(String database, String table, Function<Message, Boolean> filterFunc);
}
