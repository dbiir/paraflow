package cn.edu.ruc.iir.paraflow.connector.impl;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class ParaflowPrestoConfig
{
    private String metaserverHost;
    private int metaserverPort;
    private String hdfsWarehouse;
    private boolean fiberEnable;

    @NotNull
    public boolean getFiberEnable()
    {
        return fiberEnable;
    }

    @Config("fiber.enabled")
    @ConfigDescription("fiber enabled")
    public void setFiberEnable(boolean fiberEnable)
    {
        this.fiberEnable = fiberEnable;
    }

    @NotNull
    public String getMetaserverHost()
    {
        return metaserverHost;
    }

    @Config("metaserver.host")
    @ConfigDescription("metaserver host")
    public void setMetaserverHost(String metaserverHost)
    {
        this.metaserverHost = requireNonNull(metaserverHost);
    }

    @NotNull
    public int getMetaserverPort()
    {
        return metaserverPort;
    }

    @Config("metaserver.port")
    @ConfigDescription("metaserver port")
    public void setMetaserverPort(int metaserverPort)
    {
        this.metaserverPort = metaserverPort;
    }

    @NotNull
    public String getHDFSWarehouse()
    {
        return hdfsWarehouse;
    }

    @Config("hdfs.warehouse")
    @ConfigDescription("HDFS warehouse")
    public void setHDFSWarehouse(String hdfsWarehouse)
    {
        this.hdfsWarehouse = requireNonNull(hdfsWarehouse);
    }
}
