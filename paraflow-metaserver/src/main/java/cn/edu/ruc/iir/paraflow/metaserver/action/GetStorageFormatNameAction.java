package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.StorageFormatNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetStorageFormatNameAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> sfIdOp = input.getProperties("sfId");
        Optional<Object> paramOp = input.getParam();
        if (paramOp.isPresent() && sfIdOp.isPresent()) {
            long sfId = (long) sfIdOp.get();
            String sqlStatement = SQLTemplate.findStorageFormatName(sfId);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("sfName", resultList.get(0).get(0));
            }
            else {
                throw new StorageFormatNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
