package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.FiberFuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetFiberFuncIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ActionResponse response = new ActionResponse();
        Optional<Object> paramOp = input.getParam();
        if (paramOp.isPresent()) {
            MetaProto.TblParam tblParam = (MetaProto.TblParam) paramOp.get();
            String sqlStatement = SQLTemplate.findFiberFuncId(tblParam.getFiberFuncName());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                response.setProperties("funcId", resultList.get(0).get(0));
            }
            else {
                throw new FiberFuncNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return response;
    }
}
