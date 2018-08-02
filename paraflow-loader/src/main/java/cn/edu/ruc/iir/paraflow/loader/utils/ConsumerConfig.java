package cn.edu.ruc.iir.paraflow.loader.utils;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumerConfig
{
    private ParaFlowConfig paraflowConfig;
    private ConsumerConfig()
    {}

    private static class MetaConfigHolder
    {
        private static final ConsumerConfig instance = new ConsumerConfig();
    }

    public static final ConsumerConfig INSTANCE()
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
            getGroupId();
            getKafkaBootstrapServers();
            getKafkaKeyDeserializerClass();
            getKafkaValueDeserializerClass();
            getKafkaPartitionerClass();
            getConsumerShutdownTimeout();
            getMetaClientShutdownTimeout();
            getKafkaThreadNum();
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

    public String getKafkaBootstrapServers()
    {
        return paraflowConfig.getProperty("bootstrap.servers");
    }

    public int getMetaServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.server.port"));
    }

    public String getMetaServerHost()
    {
        return paraflowConfig.getProperty("meta.server.host");
    }

    public String getGroupId()
    {
        return paraflowConfig.getProperty("consumer.group.id");
    }

    public String getKafkaKeyDeserializerClass()
    {
        return paraflowConfig.getProperty("key.deserializer");
    }

    public String getKafkaValueDeserializerClass()
    {
        return paraflowConfig.getProperty("value.deserializer");
    }

    public String getKafkaPartitionerClass()
    {
        return paraflowConfig.getProperty("kafka.partitioner");
    }

    public int getKafkaThreadNum()
    {
        return Integer.parseInt(paraflowConfig.getProperty("consumer.thread.num"));
    }

    public int getDataPullThreadNum()
    {
        return Integer.parseInt(paraflowConfig.getProperty("consumer.pull.thread.num"));
    }

    public int getDataProcessThreadNum()
    {
        return Integer.parseInt(paraflowConfig.getProperty("consumer.process.thread.num"));
    }

    public int getDataFlushThreadNum()
    {
        return Integer.parseInt(paraflowConfig.getProperty("consumer.flush.thread.num"));
    }

    public int getConsumerShutdownTimeout()
    {
        return Integer.parseInt(paraflowConfig.getProperty("consumer.shutdown.timeout"));
    }

    public String getHDFSWarehouse()
    {
        if (paraflowConfig.getProperty("hdfs.warehouse").endsWith("/")) {
            return paraflowConfig.getProperty("hdfs.warehouse").substring(
                    0, paraflowConfig.getProperty("hdfs.warehouse").length() - 2);
        }
        else {
            return paraflowConfig.getProperty("hdfs.warehouse");
        }
    }

    public long getBufferPoolSize()
    {
        return Long.parseLong(paraflowConfig.getProperty("consumer.buffer.pool.size"));
    }

    public int getMetaClientShutdownTimeout()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.client.shutdown.timeout"));
    }

    public long getConsumerPollTimeout()
    {
        return Long.parseLong(paraflowConfig.getProperty("consumer.poll.timeout.ms"));
    }

    public long getOrcFileStripeSize()
    {
        return Long.parseLong(paraflowConfig.getProperty("orc.file.stripe.size"));
    }

    public int getOrcFileBufferSize()
    {
        return Integer.parseInt(paraflowConfig.getProperty("orc.file.buffer.size"));
    }

    public long getOrcFileBlockSize()
    {
        return Long.parseLong(paraflowConfig.getProperty("orc.file.block.size"));
    }
}
