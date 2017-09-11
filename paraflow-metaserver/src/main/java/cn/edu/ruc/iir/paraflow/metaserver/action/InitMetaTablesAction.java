package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaTableInitException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;

/**
 * paraflow
 *
 * @author guodong
 */
public class InitMetaTablesAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        // 1. check if ver has values
        // 2. if has values, validate the ver
        // 3. if has no values, insert init values
        // todo deal with meta table init
        ResultList metaTableList = input.getResponseResultList();
        // if meta data already exist
        if (metaTableList.size() == MetaConstants.metaTableNum) {
            // todo alice: validate meta data
            return new ActionResponse();
        }
        if (metaTableList.size() == 0) {
            String[] statements = new String[2];
            statements[0] = MetaConstants.initVerTableSql;
            statements[1] = MetaConstants.initFiberFuncSql;
            int[] results = connection.executeUpdateInBatch(statements);
            for (int res : results) {
                if (res != 1) {
                    throw new MetaTableInitException();
                }
            }
        }
        return new ActionResponse();
    }
}
