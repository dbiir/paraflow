package cn.edu.ruc.iir.paraflow.connector.impl;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ConfigFactory;
import com.facebook.presto.spi.PrestoException;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.log.Logger;

import javax.validation.constraints.NotNull;

import java.io.IOException;

import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.PARAFLOW_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class ParaflowPrestoConfig
{
    private Logger logger = Logger.get(ParaflowPrestoConfig.class);

    private String jdbcDriver;
    private String metaserverUri;
    private String metaserverUser;
    private String metaserverPass;
    private String metaserverStore;
    private String configPath;

    @NotNull
    public String getJdbcDriver()
    {
        return jdbcDriver;
    }

    @NotNull
    public String getMetaserverUri()
    {
        return metaserverUri;
    }

    @NotNull
    public String getMetaserverUser()
    {
        return metaserverUser;
    }

    @NotNull
    public String getMetaserverPass()
    {
        return metaserverPass;
    }

    @NotNull
    public String getMetaserverStore()
    {
        return metaserverStore;
    }

    @NotNull
    public String getConfigPath()
    {
        return configPath;
    }

    @Config("hdfs.metaserver.driver")
    @ConfigDescription("HDFS metaserver jdbc driver")
    public void setJdbcDriver(String jdbcDriver)
    {
        this.jdbcDriver = requireNonNull(jdbcDriver);
    }

    @Config("hdfs.metaserver.uri")
    @ConfigDescription("HDFS metaserver uri")
    public void setMetaserverUri(String metaserverUri)
    {
        this.metaserverUri = requireNonNull(metaserverUri);
    }

    @Config("hdfs.metaserver.user")
    @ConfigDescription("HDFS metaserver user name")
    public void setMetaserverUser(String metaserverUsere)
    {
        this.metaserverUser = requireNonNull(metaserverUsere);
    }

    @Config("hdfs.metaserver.pass")
    @ConfigDescription("HDFS metaserver user password")
    public void setMetaserverPass(String metaserverPass)
    {
        this.metaserverPass = requireNonNull(metaserverPass);
    }

    @Config("hdfs.metaserver.store")
    @ConfigDescription("HDFS metaserver storage dir")
    public void setMetaserverStore(String metaserverStore)
    {
        this.metaserverStore = requireNonNull(metaserverStore);
    }

    @Config("paraflow.config.path")
    @ConfigDescription("Paraflow config path")
    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    private ConfigFactory configFactory = null;

    private String paraflowHome = null;

    @Config("paraflow.home")
    public ParaflowPrestoConfig setParaflowHome (String paraflowHome)
    {
        this.paraflowHome = paraflowHome;

        // reload configuration
        if (this.configFactory == null)
    {
        if (paraflowHome == null || paraflowHome.isEmpty())
        {
            String defaultParaflowHome = ConfigFactory.Instance().getProperty("paraflow.home");
            if (defaultParaflowHome == null)
            {
                logger.info("use paraflow.properties inside jar.");
            } else
            {
                logger.info("use paraflow.properties under default paraflow.home: " + defaultParaflowHome);
            }
        } else
        {
            if (!(paraflowHome.endsWith("/") || paraflowHome.endsWith("\\")))
            {
                paraflowHome += "/";
            }
            try
            {
                ConfigFactory.Instance().loadProperties(paraflowHome + "paraflow.properties");
                ConfigFactory.Instance().addProperty("paraflow.home", paraflowHome);
                logger.info("use paraflow.properties under connector specified paraflow.home: " + paraflowHome);

            } catch (IOException e)
            {
                logger.error(e,"can not load paraflow.properties under: " + paraflowHome +
                        ", configuration reloading is skipped.");
                throw new PrestoException(PARAFLOW_CONFIG_ERROR, e);
            }
        }

        configFactory = ConfigFactory.Instance();
    }
        return this;
}

    @NotNull
    public String getParaflowHome ()
    {
        return this.paraflowHome;
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return
     */
    @NotNull
    public ConfigFactory getFactory ()
    {
        return configFactory;
    }

    public String getHDFSWarehouse()
    {
        return this.metaserverStore;
    }
}
