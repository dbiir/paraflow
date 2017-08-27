package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

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
        Optional<Object> paramOp = input.getParam();
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> tblNameOp = input.getProperties("tblName");
        if (paramOp.isPresent()
                && dbIdOp.isPresent()
                && tblNameOp.isPresent()) {
            long dbId = (long) dbIdOp.get();
            String tblName = tblNameOp.get().toString();
            String sqlStatement = SQLTemplate.findTblId(dbId, tblName);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("tblId", Long.parseLong(resultList.get(0).get(0)));
            }
            else {
                throw new TableNotFoundException(tblName);
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
