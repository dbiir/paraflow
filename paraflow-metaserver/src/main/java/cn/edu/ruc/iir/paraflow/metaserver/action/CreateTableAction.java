package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TableCreationException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;
import cn.edu.ruc.iir.paraflow.metaserver.utils.Utils;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class CreateTableAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> userIdOp = input.getProperties("userId");
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> sfIdOp = input.getProperties("sfId");
        Optional<Object> funcIdOp = input.getProperties("funcId");
        if (paramOp.isPresent()
                && userIdOp.isPresent()
                && dbIdOp.isPresent()
                && sfIdOp.isPresent()
                && funcIdOp.isPresent()) {
            MetaProto.TblParam tblParam = (MetaProto.TblParam) paramOp.get();
            input.setProperties("tblName", tblParam.getTblName());
            String locationUrl = tblParam.getLocationUrl();
            //table locationurl
            if (locationUrl.isEmpty()) {
                locationUrl = Utils.formatTblUrl(tblParam.getDbName(), tblParam.getTblName());
            }
            //create table SQL
            String userStatement = SQLTemplate.createTable(
                    (Integer) dbIdOp.get(),
                    tblParam.getTblName(),
                    tblParam.getTblType(),
                    (Integer) userIdOp.get(),
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    locationUrl,
                    (Integer) sfIdOp.get(),
                    tblParam.getFiberColId(),
                    (Integer) funcIdOp.get());
            int status = connection.executeUpdate(userStatement);
            if (status == 0) {
                throw new TableCreationException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
