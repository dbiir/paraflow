package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserCreationException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class CreateUserAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> param = input.getParam();
        if (param.isPresent()) {
            MetaProto.UserParam userParam = (MetaProto.UserParam) param.get();
            String userStatement = SQLTemplate.createUser(
                    userParam.getUserName(),
                    userParam.getPassword(),
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
            int status = connection.executeUpdate(userStatement);
            if (status == 0) {
                throw new UserCreationException();
            }
        }
        return new ActionResponse();
    }
}
