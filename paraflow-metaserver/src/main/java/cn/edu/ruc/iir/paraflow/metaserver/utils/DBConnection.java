package cn.edu.ruc.iir.paraflow.metaserver.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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
    private Connection connection;
    private static DBConnection connectionInstance = null;

    public static DBConnection getConnectionInstance()
    {
        if (connectionInstance == null) {
            connectionInstance = new DBConnection();
        }

        return connectionInstance;
    }

    public static void connect(String driver, String host, String user, String password)
    {
        DBConnection.driver = driver;
        DBConnection.host = host;
        DBConnection.user = user;
        DBConnection.password = password;
    }

    public void close()
    {
    }

    private DBConnection()
    {
        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(host, user, password);
            this.connection.setAutoCommit(false);
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    public void SqlExecutor(String sqlStatement)
    {
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sqlStatement);
            stmt.close();
            connection.commit();
            connection.close();
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

        public static void main(String[] args)
    {
        connect(args[1], args[2], args[3], args[4]);
        DBConnection dbConnection = new DBConnection();
        dbConnection.SqlExecutor(args[4]);
    }
}
