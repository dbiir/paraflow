package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;

public abstract class Connection
{
    public abstract int executeUpdate(String statement) throws ParaFlowException;

    public abstract int[] executeUpdateInBatch(String[] statements) throws ParaFlowException;

    public abstract ResultList executeQuery(String statement) throws ParaFlowException;

    public abstract void setAutoCommit(boolean autoCommit) throws ParaFlowException;

    public abstract void commit() throws ParaFlowException;

    public abstract void rollback() throws ParaFlowException;

    public abstract void close() throws ParaFlowException;
}
