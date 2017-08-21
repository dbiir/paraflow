package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.metaserver.action.Action;
import cn.edu.ruc.iir.paraflow.metaserver.action.ActionResponse;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Connection
{
    public abstract <T> ActionResponse<T> execute(Action action);

    public abstract void setAutoCommit(boolean autoCommit);

    public abstract void commit();
}
