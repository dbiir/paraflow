package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.commons.exceptions.SQLExecutionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * ParaFlow
 * This is a db connection instance.
 * This is NOT thread safe!!!
 */
public class DBConnection extends cn.edu.ruc.iir.paraflow.metaserver.connection.Connection
{
    private Connection connection;

    public DBConnection(Connection connection)
    {
        this.connection = connection;
    }

    public int executeUpdate(String sqlStatement) throws SQLExecutionException
    {
        int rowNumber;
        try {
            Statement stmt = connection.createStatement();
            rowNumber = stmt.executeUpdate(sqlStatement);
            stmt.close();
        }
        catch (SQLException e) {
            throw new SQLExecutionException(sqlStatement);
        }
        return rowNumber;
    }

    public int[] executeUpdateInBatch(String[] sqlStatements) throws SQLExecutionException
    {
        try {
            Statement stmt = connection.createStatement();
            for (String sqlStatement : sqlStatements) {
                stmt.addBatch(sqlStatement);
            }
            return stmt.executeBatch();
        }
        catch (SQLException e) {
            throw new SQLExecutionException(Arrays.toString(sqlStatements));
        }
    }

    public ResultList executeQuery(String sqlStatement) throws SQLExecutionException
    {
        ResultSet resultSet;
        ResultList resultList;
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sqlStatement);
            ResultSetMetaData rsMetadata = resultSet.getMetaData();
            resultList = convert(resultSet, rsMetadata.getColumnCount());
            resultSet.close();
            stmt.close();
        }
        catch (SQLException e) {
            throw new SQLExecutionException(sqlStatement);
        }
        return resultList;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLExecutionException
    {
        try {
            connection.setAutoCommit(autoCommit);
        }
        catch (SQLException e) {
            throw new SQLExecutionException("auto commit setup");
        }
    }

    public void commit() throws SQLExecutionException
    {
        try {
            connection.commit();
        }
        catch (SQLException e) {
            throw new SQLExecutionException("commit");
        }
    }

    public void rollback() throws SQLExecutionException
    {
        try {
            connection.rollback();
        }
        catch (SQLException e) {
            throw new SQLExecutionException("rollback");
        }
    }

    public void close() throws SQLExecutionException
    {
        try {
//            connection.commit();
            connection.close();
        }
        catch (SQLException e) {
            throw new SQLExecutionException("connection close");
        }
    }

    private ResultList convert(ResultSet resultSet, int colCount) throws SQLException
    {
        ResultList resultList = new ResultList();
        while (resultSet.next()) {
            JDBCRecord jdbcRecord = new JDBCRecord(colCount);
            for (int i = 0; i < colCount; i++) {
                jdbcRecord.put(resultSet.getString(i + 1), i);
            }
            resultList.add(jdbcRecord);
        }
        return resultList;
    }
}
