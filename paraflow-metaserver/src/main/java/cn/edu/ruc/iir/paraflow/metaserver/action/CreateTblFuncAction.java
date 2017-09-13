package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TblFuncCreationException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class CreateTblFuncAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> tblIdOp = input.getProperties("tblId");
        Optional<Object> funcIdOp = input.getProperties("funcId");
        if (tblIdOp.isPresent()
                && funcIdOp.isPresent()) {
            //create tblfunc SQL
            String userStatement = SQLTemplate.createTblFunc(
                    (long) tblIdOp.get(),
                    (long) funcIdOp.get());
            int status = connection.executeUpdate(userStatement);
            if (status == 0) {
                throw new TblFuncCreationException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
