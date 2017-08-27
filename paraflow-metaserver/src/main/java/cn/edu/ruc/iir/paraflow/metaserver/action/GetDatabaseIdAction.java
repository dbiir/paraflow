package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.DatabaseNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetDatabaseIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> dbNameOp = input.getProperties("dbName");
        if (paramOp.isPresent() && dbNameOp.isPresent()) {
            String sqlStatement = SQLTemplate.findDbId(dbNameOp.get().toString());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("dbId", Long.parseLong(resultList.get(0).get(0)));
            }
            else {
                throw new DatabaseNotFoundException(dbNameOp.get().toString());
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
