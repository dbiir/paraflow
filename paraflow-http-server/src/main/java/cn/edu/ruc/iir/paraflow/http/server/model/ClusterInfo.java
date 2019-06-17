package cn.edu.ruc.iir.paraflow.http.server.model;

/**
 * paraflow
 *
 * @author guodong
 */
public class ClusterInfo
{
    private String version;
    private String uptime;

    public ClusterInfo()
    {
        this.version = "2.0-alpha";
        this.uptime = "100h";
    }

    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    public String getUptime()
    {
        return uptime;
    }

    public void setUptime(String uptime)
    {
        this.uptime = uptime;
    }
}
