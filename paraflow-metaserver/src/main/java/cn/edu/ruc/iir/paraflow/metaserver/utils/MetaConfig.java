package cn.edu.ruc.iir.paraflow.metaserver.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaConfig
{
    private ParaFlowConfig paraflowConfig;
    int serverPort;

    public MetaConfig()
    {}

    private static class MetaConfigHolder
    {
        public static final MetaConfig instance = new MetaConfig();
    }

    public static final MetaConfig INSTANCE()
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
        // TODO alice: validate configuration content
        try {
            //dbDriver format check
            String dbDriver = getDBDriver();
            String regExDriver = "^[a-zA-Z]+.[a-zA-Z]+.[a-zA-Z]+$";
            Pattern pDriver = Pattern.compile(regExDriver);
            Matcher mDriver = pDriver.matcher(dbDriver);
            boolean driver = mDriver.find();
            //dbHost format check
            String dbHost = getDBHost();
            String regExHost = "^[a-zA-Z]+:[a-zA-Z]+://(([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3})|[a-zA-Z0-9]+)(:[0-9]{0,5})?(/[a-zA-Z]+)+$";
            Pattern pHost = Pattern.compile(regExHost);
            Matcher mHost = pHost.matcher(dbHost);
            boolean host = mHost.find();
            //dbUser format check
            String dbUser = getDBUser();
            String regExUser = "^[a-zA-Z][a-zA-Z0-9]*$";
            Pattern pUser = Pattern.compile(regExUser);
            Matcher mUser = pUser.matcher(dbUser);
            boolean user = mUser.find();
            //HDFSWarehouse format check
            String hdfsWarehouse = getHDFSWarehouse();
            String regExWarehouse = "^[a-zA-Z]+://(([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3})|[a-zA-Z0-9]+)(:[0-9]{0,5})?(/[a-zA-Z]+)+$";
            Pattern pWarehouse = Pattern.compile(regExWarehouse);
            Matcher mWarehouse = pWarehouse.matcher(hdfsWarehouse);
            boolean warehouse = mWarehouse.find();
            if (driver & host & user & warehouse) {
                return true;
            }
            else {
                return false;
            }
        }
        catch (Exception e) {
            return false;
        }
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
}
