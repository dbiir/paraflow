package cn.edu.ruc.iir.paraflow.metaserver.connection;

/**
 * This is a pool for connection.
 * Currently maintains only one connection instance.
 */
public class ConnectionPool
{
    // todo implement simple connection pool
    private TransactionController curTxController = null;

    private static class ConnectionPoolHolder
    {
        private static ConnectionPool instance = new ConnectionPool();
    }

    public static final ConnectionPool INSTANCE()
    {
        return ConnectionPoolHolder.instance;
    }

    private ConnectionPool()
    {}

    public void initialize()
    {
    }

    public TransactionController getTxController()
    {
        return curTxController;
    }

    public void close()
    {}
}
