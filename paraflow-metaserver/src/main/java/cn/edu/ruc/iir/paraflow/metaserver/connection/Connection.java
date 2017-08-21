package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.metaserver.action.Action;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Connection
{
    private final TransactionController txController;

    public Connection(TransactionController txController)
    {
        this.txController = txController;
    }

    public TransactionController getTxController()
    {
        return txController;
    }

    public void execute(Action action)
    {
    }
}
