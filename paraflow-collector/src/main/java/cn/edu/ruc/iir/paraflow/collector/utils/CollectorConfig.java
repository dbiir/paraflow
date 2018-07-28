package cn.edu.ruc.iir.paraflow.collector.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * paraflow
 *
 * @author guodong
 */
public class CollectorConfig
{
    private ParaFlowConfig paraflowConfig;

    private CollectorConfig()
    {}

    private static class MetaConfigHolder
    {
        private static final CollectorConfig instance = new CollectorConfig();
    }

    public static final CollectorConfig INSTANCE()
    {
        return MetaConfigHolder.instance;
    }

    public void init(String configPath) throws ConfigFileNotFoundException
    {
        paraflowConfig = new ParaFlowConfig(configPath);
        paraflowConfig.build();
    }

    public boolean validate()
    {
        // todo alice: validate configuration content
        try {
            String str = getMetaServerHost();
            String regEx = "^[a-zA-Z]+://[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{1,5}(/[a-zA-Z]+)+$";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            boolean result = m.find();
            if (!result) {
                return false;
            }
            getMetaServerPort();
            getBufferPollTimeout();
            getBufferOfferTimeout();
            getKafkaThreadNum();
            getProducerShutdownTimeout();
            getMetaClientShutdownTimeout();
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    public Properties getProperties()
    {
        return paraflowConfig.getProperties();
    }

    public int getMetaServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.server.port"));
    }

    public String getMetaServerHost()
    {
        return paraflowConfig.getProperty("meta.server.host");
    }

    public long getBufferPollTimeout()
    {
        return Long.parseLong(paraflowConfig.getProperty("producer.buffer.poll.timeout"));
    }

    public long getBufferOfferTimeout()
    {
        return Long.parseLong(paraflowConfig.getProperty("producer.buffer.offer.timeout"));
    }

    public int getKafkaThreadNum()
    {
        return Integer.parseInt(paraflowConfig.getProperty("producer.thread.num"));
    }

    public String getKafkaBootstrapServers()
    {
        return paraflowConfig.getProperty("bootstrap.servers");
    }

    public String getKafkaAcks()
    {
        return paraflowConfig.getProperty("acks");
    }

    public int getKafkaRetries()
    {
        return Integer.parseInt(paraflowConfig.getProperty("retries"));
    }

    public int getKafkaBatchSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("batch.size"));
    }

    public int getKafkaLingerMs()
    {
        return Integer.parseInt(paraflowConfig.getProperty("linger.ms"));
    }

    public int getKafkaBufferMem()
    {
        return Integer.parseInt(paraflowConfig.getProperty("buffer.memory"));
    }

    public String getKafkaKeySerializerClass()
    {
        return paraflowConfig.getProperty("key.serializer");
    }

    public String getKafkaValueSerializerClass()
    {
        return paraflowConfig.getProperty("value.serializer");
    }

    public String getKafkaPartitionerClass()
    {
        return paraflowConfig.getProperty("partitioner");
    }

    public int getProducerShutdownTimeout()
    {
        return Integer.parseInt(paraflowConfig.getProperty("producer.shutdown.timeout"));
    }

    public int getMetaClientShutdownTimeout()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.client.shutdown.timeout"));
    }
}
