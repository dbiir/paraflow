package cn.edu.ruc.iir.paraflow.loader.utils;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.Properties;

public class LoaderConfig
{
    private ParaFlowConfig paraflowConfig;
    private LoaderConfig()
    {}

    private static class MetaConfigHolder
    {
        private static final LoaderConfig instance = new LoaderConfig();
    }

    public static final LoaderConfig INSTANCE()
    {
        return MetaConfigHolder.instance;
    }

    public void init() throws ConfigFileNotFoundException
    {
        paraflowConfig = new ParaFlowConfig("loader.conf");
        paraflowConfig.build();
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
