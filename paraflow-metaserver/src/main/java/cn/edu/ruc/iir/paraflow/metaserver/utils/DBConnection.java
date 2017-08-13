package cn.edu.ruc.iir.paraflow.metaserver.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;

/**
 * ParaFlow
 * This is a db connection instance.
 * This is NOT thread safe!!!
 *
 * @author guodong
 */

public class DBConnection
{
//    private static String driver;
//    private static String host;
//    private static String user;
//    private static String password;
    private Connection connection;
    private static DBConnection connectionInstance = null;

    public static DBConnection getConnectionInstance()
    {
        if (connectionInstance == null) {
            connectionInstance = new DBConnection();
        }

        return connectionInstance;
    }

    public void connect(String driver, String host, String user, String password)
    {
//        DBConnection.driver = driver;
//        DBConnection.host = host;
//        DBConnection.user = user;
//        DBConnection.password = password;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(host, user, password);
            connection.setAutoCommit(true);
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    private DBConnection()
    {
    }

    public Optional sqlUpdate(String sqlStatement)
    {
        int rowNumber = 0;
        try {
            Statement stmt = connection.createStatement();
            System.out.println("connection.createStatement");
            rowNumber = stmt.executeUpdate(sqlStatement);
            System.out.println("stmt.executeUpdate");
            stmt.close();
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        Optional option = Optional.ofNullable(rowNumber);
        if (option.isPresent() == true) {
            return option;
        }
        else {
            return null;
        }
    }

    public Optional<ResultSet> sqlQuery(String sqlStatement)
    {
        System.out.println(sqlStatement);
        ResultSet resultSet = null;
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sqlStatement);
//            stmt.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.out.println("sqlQuery java.sql.SQLException");
            System.exit(0);
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.out.println("sqlQuery NullPointerException");
            System.exit(0);
        }
        if (resultSet != null) {
            return Optional.of(resultSet);
        }
        else {
            return Optional.empty();
        }
    }

    public void close()
    {
        try {
            connection.commit();
            connection.close();
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
