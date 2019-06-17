package cn.edu.ruc.iir.paraflow.collector.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.Properties;

/**
 * paraflow
 *
 * @author guodong
 */
public class CollectorConfig
{
    private ParaFlowConfig paraflowConfig;

    private CollectorConfig()
    {
    }

    public static final CollectorConfig INSTANCE()
    {
        return MetaConfigHolder.instance;
    }

    public void init() throws ConfigFileNotFoundException
    {
        paraflowConfig = new ParaFlowConfig("collector.conf");
        paraflowConfig.build();
    }

    public Properties getProperties()
    {
        return paraflowConfig.getProperties();
    }

    public String getCollectorId()
    {
        return paraflowConfig.getProperty("collector.id");
    }

    public int getMetaServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.server.port"));
    }

    public String getMetaServerHost()
    {
        return paraflowConfig.getProperty("meta.server.host");
    }

    public String getKafkaBootstrapServers()
    {
        return paraflowConfig.getProperty("bootstrap.servers");
    }

    public boolean isMetricEnabled()
    {
        return paraflowConfig.getProperty("metric.enabled").equalsIgnoreCase("true");
    }

    public String getPushGateWayUrl()
    {
        return paraflowConfig.getProperty("gateway.url");
    }

    public int getMetaClientShutdownTimeout()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.client.shutdown.timeout"));
    }

    private static class MetaConfigHolder
    {
        private static final CollectorConfig instance = new CollectorConfig();
    }
}
