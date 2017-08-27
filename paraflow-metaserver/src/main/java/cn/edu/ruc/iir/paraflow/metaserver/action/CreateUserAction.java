package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserCreationException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class CreateUserAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        if (paramOp.isPresent()) {
            MetaProto.UserParam userParam = (MetaProto.UserParam) paramOp.get();
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
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
