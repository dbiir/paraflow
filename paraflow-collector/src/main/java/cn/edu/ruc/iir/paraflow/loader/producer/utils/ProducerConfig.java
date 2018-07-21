package cn.edu.ruc.iir.paraflow.loader.producer.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * paraflow
 *
 * @author guodong
 */
public class ProducerConfig
{
    private ParaFlowConfig paraflowConfig;

    private ProducerConfig()
    {}

    private static class MetaConfigHolder
    {
        private static final ProducerConfig instance = new ProducerConfig();
    }

    public static final ProducerConfig INSTANCE()
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
        return paraflowConfig.getProperty("kafka.bootstrap.servers");
    }

    public String getKafkaAcks()
    {
        return paraflowConfig.getProperty("kafka.acks");
    }

    public int getKafkaRetries()
    {
        return Integer.parseInt(paraflowConfig.getProperty("kafka.retries"));
    }

    public int getKafkaBatchSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("kafka.batch.size"));
    }

    public int getKafkaLingerMs()
    {
        return Integer.parseInt(paraflowConfig.getProperty("kafka.linger.ms"));
    }

    public int getKafkaBufferMem()
    {
        return Integer.parseInt(paraflowConfig.getProperty("kafka.buffer.memory"));
    }

    public String getKafkaKeySerializerClass()
    {
        return paraflowConfig.getProperty("kafka.key.serializer");
    }

    public String getKafkaValueSerializerClass()
    {
        return paraflowConfig.getProperty("kafka.value.serializer");
    }

    public String getKafkaPartitionerClass()
    {
        return paraflowConfig.getProperty("kafka.partitioner");
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
