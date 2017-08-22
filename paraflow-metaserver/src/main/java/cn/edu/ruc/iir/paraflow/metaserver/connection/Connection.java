package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Connection
{
    public abstract int executeUpdate(String statement);
    public abstract List<Integer> executeUpdateInBatch(String[] statements);
    public abstract ResultList executeQuery(String statement);
    public abstract void setAutoCommit(boolean autoCommit);
    public abstract void commit();
    public abstract void rollback();
}
