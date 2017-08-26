package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;

public abstract class Action
{
    public abstract ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException;
}
