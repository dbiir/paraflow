package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.FuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetFuncIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> funcNameOp = input.getProperties("funcName");
        if (paramOp.isPresent() && funcNameOp.isPresent()) {
            String sqlStatement = SQLTemplate.findFuncId(funcNameOp.get().toString());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("funcId", Long.parseLong(resultList.get(0).get(0)));
            }
            else {
                throw new FuncNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
