package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class GetUserIdAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection)
            throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> userNameOp = input.getProperties("userName");
        if (paramOp.isPresent() && userNameOp.isPresent()) {
            String userName = userNameOp.get().toString();
            String sqlStatement = SQLTemplate.findUserId(userName);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                input.setProperties("userId", Long.parseLong(resultList.get(0).get(0)));
            }
            else {
                throw new UserNotFoundException();
            }
            return input;
        }
        else {
            throw new ActionParamNotValidException();
        }
    }
}
