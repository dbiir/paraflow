package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.metaserver.action.Action;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class TransactionController
{
    private final Connection connection;
    private final List<Action> actions;
    private boolean autoCommit = true;

    public TransactionController(Connection connection)
    {
        this.connection = connection;
        this.actions = new ArrayList<>();
    }

    public void setAutoCommit(boolean autoCommit)
    {
        this.autoCommit = autoCommit;
    }

    public void addAction(Action action)
    {
        actions.add(action);
    }

    public void commit()
    {
        connection.setAutoCommit(autoCommit);
        actions.forEach(connection::execute);
        connection.commit();
    }
}
