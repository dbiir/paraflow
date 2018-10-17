package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaTableCorruptedException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaTableCreationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;

/**
 * Create meta table. Depends on GetMetaTablesAction
 */
public class CreateMetaTablesAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        ResultList metaTableList = input.getResponseResultList();
        // if meta data already exist
        if (metaTableList.size() == MetaConstants.metaTableNum) {
            return input;
        }
        if (metaTableList.size() == 0) {
            String[] statements = new String[MetaConstants.metaTableNum];
            statements[0] = MetaConstants.createVerModelSql;
            statements[1] = MetaConstants.createUserModelSql;
            statements[2] = MetaConstants.createDbModelSql;
            statements[3] = MetaConstants.createTblModelSql;
            statements[4] = MetaConstants.createColModelSql;
            statements[5] = MetaConstants.createDbParamModelSql;
            statements[6] = MetaConstants.createTblParamModelSql;
            statements[7] = MetaConstants.createTblPrivModelSql;
            statements[8] = MetaConstants.createBlockIndexSql;

            int[] results = connection.executeUpdateInBatch(statements);
            for (int res : results) {
                if (res != 0) {
                    throw new MetaTableCreationException();
                }
            }
            return input;
        }
        // meta data is corrupted if this statement is reached
        throw new MetaTableCorruptedException();
    }
}
