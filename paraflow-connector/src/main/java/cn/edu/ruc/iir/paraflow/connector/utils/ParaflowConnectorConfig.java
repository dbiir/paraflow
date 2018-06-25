package cn.edu.ruc.iir.paraflow.connector.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class ParaflowConnectorConfig
{
    private String jdbcDriver;
    private String metaserverUri;
    private String metaserverUser;
    private String metaserverPass;
    private String metaserverStore;

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

    public String getHDFSWarehouse() {
        ConfigFactory config = new ConfigFactory();
        try {
            config.init();
        }
        catch (ConfigFileNotFoundException ignore) {
        }
        return config.getHDFSWarehouse();
    }
}
