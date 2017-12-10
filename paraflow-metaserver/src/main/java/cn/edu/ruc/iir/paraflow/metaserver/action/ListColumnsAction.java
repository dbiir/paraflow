package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ColumnsNotExistException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.ArrayList;
import java.util.Optional;

public class ListColumnsAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> tblIdOp = input.getProperties("tblId");
        if (paramOp.isPresent() && dbIdOp.isPresent() && tblIdOp.isPresent()) {
            MetaProto.DbTblParam dbTblParam =
                    (MetaProto.DbTblParam) paramOp.get();
            String sqlStatement = SQLTemplate.listColumns((Long) dbIdOp.get(), (Long) tblIdOp.get());
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                MetaProto.StringListType stringList;
                //result
                ArrayList<String> result = new ArrayList<>();
                int size = resultList.size();
                for (int i = 0; i < size; i++) {
                    result.add(resultList.get(i).get(0));
                    System.out.println("Meta server execution result " + i + ": " + resultList.get(i).get(0));
                }
                stringList = MetaProto.StringListType.newBuilder()
                        .addAllStr(result)
                        .setIsEmpty(false)
                        .build();
                input.setParam(stringList);
            }
            else {
                throw new ColumnsNotExistException(
                        dbTblParam.getDatabase().getDatabase(),
                        dbTblParam.getTable().getTable());
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
