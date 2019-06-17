package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.action.ActionResponse;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateBlockIndexAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateColumnAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateDatabaseAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateDbParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateTableAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateTblParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateTblPrivAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateUserAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteBlockIndexAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteColumnAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDatabaseAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbBlockIndexAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbColumnAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbTableAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbTblParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteDbTblPrivAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteTableAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteTblParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.DeleteTblPrivAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.FilterBlockIndexAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.FilterBlockIndexByFiberAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetColumnAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetColumnNameAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetDatabaseAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetDatabaseIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetDbParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetDbTblIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetTableAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetTableIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetTblParamAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetUserIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetUserNameAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.ListColumnsAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.ListColumnsDataTypeAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.ListColumnsIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.ListDatabasesAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.ListTablesAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.RenameColumnAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.RenameDatabaseAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.RenameTableAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.UpdateBlockPathAction;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ConnectionPool;
import cn.edu.ruc.iir.paraflow.metaserver.connection.TransactionController;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;
import io.grpc.stub.StreamObserver;

public class MetaService extends MetaGrpc.MetaImplBase
{
    @Override
    public void listDatabases(MetaProto.NoneType none,
                              StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(none);
            txController.setAutoCommit(true);
            txController.addAction(new ListDatabasesAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType =
                    (MetaProto.StringListType) result.getParam().get();
            System.out.println("MetaService :stringList :" + stringListType);
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam,
                           StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbNameParam);
            input.setProperties("dbName", dbNameParam.getDatabase());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new ListTablesAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType =
                    (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void listColumns(MetaProto.DbTblParam dbTblParam,
                            StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblParam);
            input.setProperties("dbName", dbTblParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblParam.getTable().getTable());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new ListColumnsAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType =
                    (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void listColumnsId(MetaProto.DbTblParam dbTblParam,
                              StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblParam);
            input.setProperties("dbName", dbTblParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblParam.getTable().getTable());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new ListColumnsIdAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType =
                    (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void listColumnsDataType(MetaProto.DbTblParam dbTblParam,
                                    StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblParam);
            input.setProperties("dbName", dbTblParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblParam.getTable().getTable());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new ListColumnsDataTypeAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType =
                    (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam,
                            StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbNameParam);
            input.setProperties("dbName", dbNameParam.getDatabase());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseAction());
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetUserNameAction());
            txController.addAction(new GetDbParamAction());
            ActionResponse result = txController.commit(input);
            MetaProto.DbParam dbParam =
                    (MetaProto.DbParam) result.getParam().get();
            System.out.println("dbParam : " + dbParam);
            responseStreamObserver.onNext(dbParam);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(dbParam);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam,
                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblParam);
            input.setProperties("dbName", dbTblParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblParam.getTable().getTable());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableAction());
            txController.addAction(new GetUserNameAction());
            txController.addAction(new GetTblParamAction());
            ActionResponse result = txController.commit(input);
            MetaProto.TblParam tblParam =
                    (MetaProto.TblParam) result.getParam().get();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam,
                          StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblColParam);
            input.setProperties("dbName", dbTblColParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblColParam.getTable().getTable());
            input.setProperties("colName", dbTblColParam.getColumn().getColumn());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new GetColumnAction());
            ActionResponse result = txController.commit(input);
            MetaProto.ColParam colParam = (MetaProto.ColParam) result.getParam().get();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.ColParam colParam = MetaProto.ColParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void getColumnName(MetaProto.DbTblColIdParam dbTblColIdParam,
                              StreamObserver<MetaProto.ColNameParam> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblColIdParam);
            input.setProperties("dbId", dbTblColIdParam.getDbId());
            input.setProperties("tblId", dbTblColIdParam.getTblId());
            input.setProperties("colId", dbTblColIdParam.getColId());
            txController.setAutoCommit(true);
            txController.addAction(new GetColumnNameAction());
            ActionResponse result = txController.commit(input);
            MetaProto.ColNameParam colParam = (MetaProto.ColNameParam) result.getParam().get();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.ColNameParam colParam = MetaProto.ColNameParam.newBuilder()
                    .build();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createDatabase(MetaProto.DbParam dbParam,
                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbParam);
            input.setProperties("userName", dbParam.getUserName());
            txController.setAutoCommit(false);
            txController.addAction(new GetUserIdAction());
            txController.addAction(new CreateDatabaseAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createTable(MetaProto.TblParam tblParam,
                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(tblParam);
            input.setProperties("userName", tblParam.getUserName());
            input.setProperties("dbName", tblParam.getDbName());
            input.setProperties("sfName", tblParam.getStorageFormatName());
            input.setProperties("partitionerName", tblParam.getFuncName());
            txController.setAutoCommit(false);
            txController.addAction(new GetUserIdAction());
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new CreateTableAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new CreateColumnAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam dbNameParam,
                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbNameParam);
            input.setProperties("dbName", dbNameParam.getDatabase());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetDbTblIdAction());
            txController.addAction(new DeleteDbTblParamAction());
            txController.addAction(new DeleteDbTblPrivAction());
            txController.addAction(new DeleteDbBlockIndexAction());
            txController.addAction(new DeleteDbColumnAction());
            txController.addAction(new DeleteDbTableAction());
            txController.addAction(new DeleteDbParamAction());
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new DeleteDatabaseAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void deleteTable(MetaProto.DbTblParam dbTblParam,
                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbTblParam);
            input.setProperties("dbName", dbTblParam.getDatabase().getDatabase());
            input.setProperties("tblName", dbTblParam.getTable().getTable());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new DeleteColumnAction());
            txController.addAction(new DeleteTblParamAction());
            txController.addAction(new DeleteTblPrivAction());
            txController.addAction(new DeleteBlockIndexAction());
            txController.addAction(new DeleteTableAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbParam,
                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(renameDbParam);
            input.setProperties("newName", renameDbParam.getNewName());
            input.setProperties("oldName", renameDbParam.getOldName());
            txController.setAutoCommit(false);
            txController.addAction(new RenameDatabaseAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblParam,
                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(renameTblParam);
            input.setProperties("dbName", renameTblParam.getDatabase().getDatabase());
            input.setProperties("newName", renameTblParam.getNewName());
            input.setProperties("oldName", renameTblParam.getOldName());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new RenameTableAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn,
                             StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(renameColumn);
            input.setProperties("dbName", renameColumn.getDatabase().getDatabase());
            input.setProperties("tblName", renameColumn.getTable().getTable());
            input.setProperties("oldName", renameColumn.getOldName());
            input.setProperties("newName", renameColumn.getNewName());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new RenameColumnAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createDbParam(MetaProto.DbParamParam dbParam,
                              StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbParam);
            input.setProperties("dbName", dbParam.getDbName());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new CreateDbParamAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createTblParam(MetaProto.TblParamParam tblParam,
                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(tblParam);
            input.setProperties("dbName", tblParam.getDbName());
            input.setProperties("tblName", tblParam.getTblName());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new CreateTblParamAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createTblPriv(MetaProto.TblPrivParam tblPriv,
                              StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(tblPriv);
            input.setProperties("dbName", tblPriv.getDbName());
            input.setProperties("tblName", tblPriv.getTblName());
            input.setProperties("userName", tblPriv.getUserName());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new GetUserIdAction());
            txController.addAction(new CreateTblPrivAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createBlockIndex(MetaProto.BlockIndexParam blockIndex,
                                 StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(blockIndex);
            input.setProperties("dbName", blockIndex.getDatabase().getDatabase());
            input.setProperties("tblName", blockIndex.getTable().getTable());
            txController.setAutoCommit(false);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new CreateBlockIndexAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void updateBlockPath(MetaProto.UpdateBlockPathParam updateBlockPathParam,
                                StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(updateBlockPathParam);
            txController.setAutoCommit(false);
            txController.addAction(new UpdateBlockPathAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void createUser(MetaProto.UserParam user,
                           StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(user);
            txController.setAutoCommit(false);
            txController.addAction(new CreateUserAction());
            txController.commit(input);
            responseStreamObserver.onNext(MetaConstants.OKStatus);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            responseStreamObserver.onNext(e.getResponseStatus());
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockIndexParam,
                                 StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(filterBlockIndexParam);
            input.setProperties("dbName", filterBlockIndexParam.getDatabase().getDatabase());
            input.setProperties("tblName", filterBlockIndexParam.getTable().getTable());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new FilterBlockIndexAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType
                    = (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam,
                                        StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(filterBlockIndexByFiberParam);
            input.setProperties("dbName", filterBlockIndexByFiberParam.getDatabase().getDatabase());
            input.setProperties("tblName", filterBlockIndexByFiberParam.getTable().getTable());
            txController.setAutoCommit(true);
            txController.addAction(new GetDatabaseIdAction());
            txController.addAction(new GetTableIdAction());
            txController.addAction(new FilterBlockIndexByFiberAction());
            ActionResponse result = txController.commit(input);
            MetaProto.StringListType stringListType
                    = (MetaProto.StringListType) result.getParam().get();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (ParaFlowException e) {
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
            e.handle();
        }
        finally {
            if (txController != null) {
                txController.close();
            }
        }
    }

    @Override
    public void stopServer(MetaProto.NoneType noneType,
                           StreamObserver<MetaProto.NoneType> responseStreamObserver)
    {
        Runtime.getRuntime().exit(0);
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        responseStreamObserver.onNext(none);
        responseStreamObserver.onCompleted();
    }
}
