package cn.edu.ruc.iir.paraflow.loader.producer.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

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
        // TODO validate configuration content
        return true;
    }

    public int getServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("meta.port"));
    }

    public String getServerHost()
    {
        return paraflowConfig.getProperty("meta.host");
    }
}
