package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaTableCorruptedException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaTableCreationException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;

import java.util.List;

/**
 * Create meta table. Depends on GetMetaTablesAction
 */
public class CreateMetaTablesAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws MetaTableCorruptedException, MetaTableCreationException
    {
        ResultList metaTableList = input.getResponseResultList();
        // if meta data already exist
        if (metaTableList.size() == MetaConstants.metaTableNum) {
            return new ActionResponse();
        }

        if (metaTableList.size() == 0) {
            String[] statements = new String[MetaConstants.metaTableNum];
            statements[0] = MetaConstants.createVerModelSql;
            statements[1] = MetaConstants.createUserModelSql;
            statements[2] = MetaConstants.createDbModelSql;
            statements[3] = MetaConstants.createStorageFormatModelSql;
            statements[4] = MetaConstants.createFiberFuncModelSql;
            statements[5] = MetaConstants.createTblModelSql;
            statements[6] = MetaConstants.createColModelSql;
            statements[7] = MetaConstants.createDbParamModelSql;
            statements[8] = MetaConstants.createTblParamModelSql;
            statements[9] = MetaConstants.createTblPrivModelSql;
            statements[10] = MetaConstants.createBlockIndexSql;

            List<Integer> results = connection.executeUpdateInBatch(statements);
            for (int res : results) {
                if (res != 1) {
                    throw new MetaTableCreationException();
                }
            }

            return new ActionResponse();
        }

        // meta data is corrupted if this statement is reached
        throw new MetaTableCorruptedException();
    }
}
