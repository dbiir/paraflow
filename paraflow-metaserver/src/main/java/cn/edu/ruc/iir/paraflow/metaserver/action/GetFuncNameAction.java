package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.FuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetFuncNameAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> funcIdOp = input.getProperties("funcId");
        Optional<Object> paramOp = input.getParam();
        if (funcIdOp.isPresent() && paramOp.isPresent()) {
            long funcId = (long) funcIdOp.get();
            String sqlStatement = SQLTemplate.findFuncName(funcId);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("funcName", resultList.get(0).get(0));
            }
            else {
                throw new FuncNotFoundException(String.valueOf(funcId));
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
