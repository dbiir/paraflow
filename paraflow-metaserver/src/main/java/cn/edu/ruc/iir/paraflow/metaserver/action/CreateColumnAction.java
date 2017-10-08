package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ColumnCreationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.List;
import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class CreateColumnAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> tblIdOp = input.getProperties("tblId");
        if (paramOp.isPresent()
                && dbIdOp.isPresent()
                && tblIdOp.isPresent()) {
            MetaProto.TblParam tblParam = (MetaProto.TblParam) paramOp.get();
            long dbId = (long) dbIdOp.get();
            long tblId = (long) tblIdOp.get();
            MetaProto.ColListType colListType = tblParam.getColList();
            int colCount = colListType.getColumnCount();
            List<MetaProto.ColParam> columns = colListType.getColumnList();
            //loop for every column to insert them
            String[] batchSQLs = new String[colCount];
            for (int i = 0; i < colCount; i++) {
                MetaProto.ColParam column = columns.get(i);
                String userStatement = SQLTemplate.createColumn(i,
                        dbId,
                        column.getColName(),
                        tblId,
                        column.getColType(),
                        column.getDataType());
                batchSQLs[i] = userStatement;
            }
            int[] colExecute = connection.executeUpdateInBatch(batchSQLs);
            int sizeExecute = colExecute.length;
            for (int j = 0; j < sizeExecute; j++) {
                if (colExecute[j] == 0) {
                    throw new ColumnCreationException(columns.get(j).getColName());
                }
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
