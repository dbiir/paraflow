package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetUserNameAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ActionResponse response = new ActionResponse();
        if (input.getProperties("userId").isPresent()) {
            long userId = (Long) input.getProperties("userId").get();
            String sqlStatement = SQLTemplate.findUserName(userId);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                response.setProperties("userName", resultList.get(0).get(0));
            }
            else {
                throw new UserNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return response;
    }
}
