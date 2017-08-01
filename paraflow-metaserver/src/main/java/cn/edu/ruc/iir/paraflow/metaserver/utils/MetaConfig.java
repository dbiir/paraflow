package cn.edu.ruc.iir.paraflow.metaserver.utils;

import cn.edu.ruc.iir.paraflow.commons.utils.ConfigFactory;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaConfig
{
    private ConfigFactory configInstance = null;

    public MetaConfig(String configPath)
    {
        configInstance = ConfigFactory.getConfigInstance(configPath);
    }

    public String getDBDriver()
    {
        return configInstance.getProperty("db.driver");
    }

    public String getDBHost()
    {
        return configInstance.getProperty("db.host");
    }

    public String getDBUser()
    {
        return configInstance.getProperty("db.user");
    }

    public String getDBPassword()
    {
        return configInstance.getProperty("db.password");
    }
}
