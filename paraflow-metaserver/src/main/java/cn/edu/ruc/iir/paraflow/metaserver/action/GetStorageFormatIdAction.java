package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.StorageFormatNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetStorageFormatIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> sfNameOp = input.getProperties("sfName");
        if (paramOp.isPresent() && sfNameOp.isPresent()) {
            String sqlStatement = SQLTemplate.findStorageFormatId(sfNameOp.get().toString());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("sfId", resultList.get(0).get(0));
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
