package cn.edu.ruc.iir.paraflow.metaserver.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaConfig
{
    private ParaFlowConfig paraflowConfig;

    public MetaConfig()
    {
    }

    public static final MetaConfig INSTANCE()
    {
        return MetaConfigHolder.instance;
    }

    public void init() throws ConfigFileNotFoundException
    {
        paraflowConfig = new ParaFlowConfig("metaserver.conf");
        paraflowConfig.build();
    }

    public int getServerPort()
    {
        return Integer.parseInt(paraflowConfig.getProperty("server.port"));
    }

    public String getDBDriver()
    {
        return paraflowConfig.getProperty("db.driver");
    }

    public String getDBHost()
    {
        return paraflowConfig.getProperty("db.host");
    }

    public String getDBUser()
    {
        return paraflowConfig.getProperty("db.user");
    }

    public String getDBPassword()
    {
        return paraflowConfig.getProperty("db.password");
    }

    public String getHDFSWarehouse()
    {
        return paraflowConfig.getProperty("hdfs.warehouse");
    }

    private static class MetaConfigHolder
    {
        public static final MetaConfig instance = new MetaConfig();
    }
}
