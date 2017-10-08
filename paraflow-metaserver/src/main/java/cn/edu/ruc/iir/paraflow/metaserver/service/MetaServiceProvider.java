package cn.edu.ruc.iir.paraflow.metaserver.service;

//import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
//import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
//import cn.edu.ruc.iir.paraflow.metaserver.action.ActionResponse;
//import cn.edu.ruc.iir.paraflow.metaserver.action.CreateColumnAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.CreateTableAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.CreateTblFuncAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.GetDatabaseIdAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.GetFuncIdAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.GetStorageFormatIdAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.GetTableIdAction;
//import cn.edu.ruc.iir.paraflow.metaserver.action.GetUserIdAction;
//import cn.edu.ruc.iir.paraflow.metaserver.connection.ConnectionPool;
//import cn.edu.ruc.iir.paraflow.metaserver.connection.TransactionController;
//import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
//import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;

/**
 * paraflow
 *
 * @author guodong
 */
public class MetaServiceProvider
{
//    public StatusProto.ResponseStatus createTable(MetaProto.TblParam tblParam)
//    {
//        StatusProto.ResponseStatus status;
//        TransactionController txController = null;
//        try {
//            txController = ConnectionPool.INSTANCE().getTxController();
//            ActionResponse input = new ActionResponse();
//            input.setParam(tblParam);
//            input.setProperties("userName", tblParam.getUserName());
//            input.setProperties("dbName", tblParam.getDbName());
//            input.setProperties("sfName", tblParam.getStorageFormatName());
//            input.setProperties("funcName", tblParam.getFuncName());
//            txController.setAutoCommit(false);
//            txController.addAction(new GetUserIdAction());
//            txController.addAction(new GetDatabaseIdAction());
//            txController.addAction(new GetStorageFormatIdAction());
//            txController.addAction(new GetFuncIdAction());
//            txController.addAction(new CreateTableAction());
//            txController.addAction(new GetTableIdAction());
//            txController.addAction(new CreateColumnAction());
//            txController.addAction(new CreateTblFuncAction());
//            txController.commit(input);
//            status = MetaConstants.OKStatus;
//        }
//        catch (ParaFlowException e) {
//            status = e.getResponseStatus();
//            e.handle();
//        }
//        finally {
//            if (txController != null) {
//                txController.close();
//            }
//        }
//        return status;
//    }
}
