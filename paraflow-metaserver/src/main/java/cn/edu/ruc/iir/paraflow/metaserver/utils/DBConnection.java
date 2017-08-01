package cn.edu.ruc.iir.paraflow.metaserver.utils;

import org.javalite.activejdbc.Base;

/**
 * ParaFlow
 * This is a db connection instance.
 * This is NOT thread safe!!!
 *
 * @author guodong
 */
public class DBConnection
{
    private static String driver;
    private static String host;
    private static String user;
    private static String password;

    private static DBConnection connectionInstance = null;

    public static DBConnection getConnectionInstance()
    {
        if (connectionInstance == null) {
            connectionInstance = new DBConnection();
        }

        return connectionInstance;
    }

    private DBConnection()
    {
    }

    public static void connect(String driver, String host, String user, String password)
    {
        DBConnection.driver = driver;
        DBConnection.host = host;
        DBConnection.user = user;
        DBConnection.password = password;

        getConnectionInstance();

        Base.open(driver, host, user, password);
    }

    public void close()
    {
        Base.close();
    }
}
