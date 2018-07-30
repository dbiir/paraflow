package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ColumnNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetColumnNameAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> tblIdOp = input.getProperties("tblId");
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> colIdOp = input.getProperties("colId");
        if (tblIdOp.isPresent() && dbIdOp.isPresent() && colIdOp.isPresent()) {
            long tblId = (long) tblIdOp.get();
            long dbId = (long) dbIdOp.get();
            long colId = (long) colIdOp.get();
            String sqlStatement = SQLTemplate.getColumnName(
                    dbId,
                    tblId,
                    colId);
            System.out.println("sqlStatement : " + sqlStatement);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                MetaProto.ColNameParam column = MetaProto.ColNameParam.newBuilder()
                        .setColumn(resultList.get(0).get(0))
                        .build();
                input.setParam(column);
            }
            else {
                throw new ColumnNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
