package cn.edu.ruc.iir.paraflow.metaserver.utils;

import org.javalite.activejdbc.Base;

/**
 * ParaFlow
 * This is a per-thread db connection.
 *
 * @author guodong
 */
public class DBConnection
{
    private String driver;
    private String host;
    private String user;
    private String password;

    public DBConnection(String driver, String host, String user, String password)
    {
        this.driver = driver;
        this.host = host;
        this.user = user;
        this.password = password;
    }

    public void open()
    {
        Base.open(driver, host, user, password);
    }

    public void close()
    {
        Base.close();
    }
}
