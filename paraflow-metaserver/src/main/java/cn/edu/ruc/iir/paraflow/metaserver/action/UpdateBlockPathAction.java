package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UpdateBlockPathException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class UpdateBlockPathAction
        extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection)
            throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        if (paramOp.isPresent()) {
            MetaProto.UpdateBlockPathParam param = (MetaProto.UpdateBlockPathParam) paramOp.get();
            String updateBlockPathStatement = SQLTemplate.updateBlockPath(param.getOriginPath(), param.getNewPath());
            int status = connection.executeUpdate(updateBlockPathStatement);
            if (status == 0) {
                throw new UpdateBlockPathException();
            }
        }
        return null;
    }
}
