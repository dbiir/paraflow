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

    @NotNull
    public String getMetaserverHost()
    {
        return metaserverHost;
    }

    @NotNull
    public int getMetaserverPort()
    {
        return metaserverPort;
    }

    @NotNull
    public String getHDFSWarehouse()
    {
        return hdfsWarehouse;
    }

    @Config("metaserver.host")
    @ConfigDescription("metaserver host")
    public void setMetaserverHost(String metaserverHost)
    {
        this.metaserverHost = requireNonNull(metaserverHost);
    }

    @Config("metaserver.port")
    @ConfigDescription("metaserver port")
    public void setMetaserverPort(int metaserverPort)
    {
        this.metaserverPort = metaserverPort;
    }

    @Config("hdfs.warehouse")
    @ConfigDescription("HDFS warehouse")
    public void setHDFSWarehouse(String hdfsWarehouse)
    {
        this.hdfsWarehouse = requireNonNull(hdfsWarehouse);
    }
}
