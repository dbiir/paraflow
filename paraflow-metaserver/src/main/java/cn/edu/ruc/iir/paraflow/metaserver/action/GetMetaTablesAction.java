package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetMetaTablesAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ActionResponse response = new ActionResponse();
        response.setResponseResultList(
                connection.executeQuery(MetaConstants.getMetaTablesSql));
        return response;
    }
}
