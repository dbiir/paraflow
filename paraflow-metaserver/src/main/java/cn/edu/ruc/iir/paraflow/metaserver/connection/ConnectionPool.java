package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.commons.exceptions.SQLExecutionException;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This is a pool for connection.
 * Currently maintains only one connection instance.
 */
public class ConnectionPool
{
    private HikariDataSource dataSource = null;

    private ConnectionPool()
    {
    }

    public static final ConnectionPool INSTANCE()
    {
        return ConnectionPoolHolder.instance;
    }

    public void initialize()
    {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(MetaConfig.INSTANCE().getDBDriver());
        config.setJdbcUrl(MetaConfig.INSTANCE().getDBHost());
        config.setUsername(MetaConfig.INSTANCE().getDBUser());
        config.setPassword(MetaConfig.INSTANCE().getDBPassword());
        dataSource = new HikariDataSource(config);
    }

    public TransactionController getTxController() throws SQLExecutionException
    {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            return new TransactionController(new DBConnection(connection));
        }
        catch (SQLException e) {
            throw new SQLExecutionException("Get jdbc connection");
        }
    }

    public void close()
    {
        dataSource.close();
    }

    private static class ConnectionPoolHolder
    {
        private static ConnectionPool instance = new ConnectionPool();
    }
}
