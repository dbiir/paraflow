package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetTableIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ActionResponse response = new ActionResponse();
        if (input.getProperties("dbId").isPresent() && input.getProperties("tblName").isPresent()) {
            long dbId = (Long) input.getProperties("dbId").get();
            String tblName = (String) input.getProperties("tblName").get();
            String sqlStatement = SQLTemplate.findTblId(dbId, tblName);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                response.setProperties("tblId", resultList.get(0).get(0));
            }
            else {
                throw new TableNotFoundException(tblName);
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return response;
    }
}
