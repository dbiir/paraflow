package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.DatabaseCreationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;
import cn.edu.ruc.iir.paraflow.metaserver.utils.Utils;

import java.util.Optional;

public class CreateDatabaseAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> param = input.getParam();
        Optional<Object> userIdOp = input.getProperties("userId");
        if (param.isPresent() && userIdOp.isPresent()) {
            MetaProto.DbParam dbParam = (MetaProto.DbParam) param.get();
            String locationUrl = dbParam.getLocationUrl();
            if (locationUrl.isEmpty()) {
                locationUrl = Utils.formatDbUrl(dbParam.getDbName());
            }
            String userStatement = SQLTemplate.createDatabase(
                    dbParam.getDbName(),
                    (long) userIdOp.get(),
                    locationUrl);
            int status = connection.executeUpdate(userStatement);
            if (status == 0) {
                throw new DatabaseCreationException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
