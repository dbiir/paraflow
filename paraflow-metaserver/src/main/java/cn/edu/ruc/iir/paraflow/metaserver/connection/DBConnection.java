package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
        catch (java.lang.ClassNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
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
            PreparedStatement stmt = connection.prepareStatement(sqlStatement);
            System.out.println("connection.createStatement");
            rowNumber = stmt.executeUpdate();
            System.out.println("stmt.executeUpdate");
            stmt.close();
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

        return rowNumber;
    }

    public ResultList convert(ResultSet resultSet, int colCount)
    {
        ResultList resultList = new ResultList();
        JDBCRecord jdbcRecord = new JDBCRecord(colCount);
        try {
            while (resultSet.next()) {
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
    // TODO close resultSet and statement locally. return an JDBCRecord
    // reference: https://github.com/dbiir/presto/blob/master/presto-hdfs/src/main/java/com/facebook/presto/hdfs/jdbc/JDBCDriver.java
    public ResultList sqlQuery(String sqlStatement, int colCount)
    {
        System.out.println(sqlStatement);
        ResultSet resultSet = null;
        ResultList resultList = new ResultList();
        try {
            PreparedStatement stmt = connection.prepareStatement(sqlStatement);
            resultSet = stmt.executeQuery();
            resultList = convert(resultSet, colCount);
            // TODO convert ResultSet to ResultList
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
