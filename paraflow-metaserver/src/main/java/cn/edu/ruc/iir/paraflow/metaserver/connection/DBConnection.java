package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

/**
 * ParaFlow
 * This is a db connection instance.
 * This is NOT thread safe!!!
 *
 * @author guodong
 */
// ref: http://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html

public class DBConnection
{
    // todo refactor DBConnection to be subclass of Connection
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
            //connection.setAutoCommit(false);
        }
        catch (java.lang.ClassNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void autoCommitTrue()
    {
        try {
            connection.setAutoCommit(true);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void autoCommitFalse()
    {
        try {
            connection.setAutoCommit(false);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private DBConnection()
    {
    }

    public int sqlUpdate(String sqlStatement)
    {
        int rowNumber = 0;
        try {
            Statement stmt = connection.createStatement();
            rowNumber = stmt.executeUpdate(sqlStatement);
            stmt.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

        return rowNumber;
    }

    public int[] sqlBatch(LinkedList<String> batchSQLs) throws SQLException
    {
        Statement stmt = connection.createStatement();
        int colCount = batchSQLs.size();
        for (int i = 0; i < colCount; i++) {
            stmt.addBatch(batchSQLs.get(i));
        }
        return stmt.executeBatch();
    }

    public ResultList convert(ResultSet resultSet, int colCount)
    {
        ResultList resultList = new ResultList();
        try {
            while (resultSet.next()) {
                JDBCRecord jdbcRecord = new JDBCRecord(colCount);
                for (int i = 0; i < colCount; i++) {
                    jdbcRecord.put(resultSet.getString(i + 1), i);
                }
                resultList.add(jdbcRecord);
            }
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        return resultList;
    }

    // reference: https://github.com/dbiir/presto/blob/master/presto-hdfs/src/main/java/com/facebook/presto/hdfs/jdbc/JDBCDriver.java
    public ResultList sqlQuery(String sqlStatement, int colCount)
    {
        ResultSet resultSet;
        ResultList resultList = new ResultList();
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sqlStatement);
            resultList = convert(resultSet, colCount);
            resultSet.close();
            stmt.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        return  resultList;
    }

    public void commit()
    {
        try {
            connection.commit();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void rollback()
    {
        try {
            connection.rollback();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void close()
    {
        try {
            connection.commit();
            connection.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
