package cn.edu.ruc.iir.paraflow.metaserver.connection;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.action.Action;
import cn.edu.ruc.iir.paraflow.metaserver.action.ActionResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    public Optional<ParaFlowException> commit()
    {
        ActionResponse response = new ActionResponse();
        try {
            connection.setAutoCommit(autoCommit);
            for (Action action : actions) {
                response = action.act(response, this.connection);
            }
            connection.commit();
        }
        catch (ParaFlowException e) {
            return Optional.of(e);
        }
        return Optional.empty();
    }
}
