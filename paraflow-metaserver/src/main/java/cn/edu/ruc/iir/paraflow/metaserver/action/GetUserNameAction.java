package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetUserNameAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> userIdOp = input.getProperties("userId");
        if (paramOp.isPresent() && userIdOp.isPresent()) {
            long userId = (long) userIdOp.get();
            String sqlStatement = SQLTemplate.findUserName(userId);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("userName", resultList.get(0).get(0));
            }
            else {
                throw new UserNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
