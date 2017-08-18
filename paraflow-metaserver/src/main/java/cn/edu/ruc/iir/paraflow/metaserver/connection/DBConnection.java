package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * ParaFlow
 * This is a db connection instance.
 * This is NOT thread safe!!!
 *
 * @author guodong
 */

// TODO catch explicit exceptions in detail

// TODO change autoCommit to false. call commit() in meta service manually
// ref: http://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html

public class DBConnection
{
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
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(host, user, password);
            connection.setAutoCommit(false);
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private DBConnection()
    {
    }

    // TODO use preparedStatement() instead of createStatement()
    public int sqlUpdate(String sqlStatement)
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

        return rowNumber;
    }

    // TODO close resultSet and statement locally. return an JDBCRecord
    // reference: https://github.com/dbiir/presto/blob/master/presto-hdfs/src/main/java/com/facebook/presto/hdfs/jdbc/JDBCDriver.java
    public Optional<ResultList> sqlQuery(String sqlStatement)
    {
        System.out.println(sqlStatement);
        ResultSet resultSet = null;
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sqlStatement);
            // TODO convert ResultSet to ResultList
            resultSet.close();
            stmt.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.out.println("sqlQuery java.sql.SQLException");
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.out.println("sqlQuery NullPointerException");
        }
        if (resultSet != null) {
            return Optional.of(resultSet);
        }
        else {
            return Optional.empty();
        }
    }

    public void commit()
    {
        try
        {
            connection.commit();
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public void rollback()
    {
        try
        {
            connection.rollback();
        } catch (SQLException e)
        {
            e.printStackTrace();
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
