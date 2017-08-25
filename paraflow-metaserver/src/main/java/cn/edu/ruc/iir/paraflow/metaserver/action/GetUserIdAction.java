package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetUserIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ActionResponse response = new ActionResponse();
        if (input.getParam().isPresent()) {
            response.setParam(input.getParam().get());
            MetaProto.DbParam dbParam = (MetaProto.DbParam) input.getParam().get();
            String sqlStatement = SQLTemplate.findUserId(dbParam.getUserName());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                response.setProperties("userId", resultList.get(0).get(0));
            }
            else {
                throw new UserNotFoundException();
            }
            return response;
        }
        else {
            throw new ActionParamNotValidException();
        }
    }
}
