package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.message.MessageParser;

import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class LoaderProducer
{
    public static class Builder
    {
        private String configPath;
        private MessageParser messageParser;
        private String kafkaProducerAcks;
        private String kafkaProducerRetries;
        private String kafkaProducerBatchSize;
        private String kafkaProducerBufferMem;
    }

    public int send(String database, String table, Message message)
    {
        // NoMappingFoundException should be thrown if table if not mapping with any topics
        return 0;
    }

    public void setTblTopicMapping(String database, String table, String host, String topic)
    {}

    public void createDatabase()
    {}

    public void createTable()
    {}

    public void createFiberFunc(String funcName, Function<Long, Integer> func)
    {
    }
}
