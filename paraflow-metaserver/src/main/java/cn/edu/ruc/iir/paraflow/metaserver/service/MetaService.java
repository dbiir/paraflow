package cn.edu.ruc.iir.paraflow.metaserver.service;

//import cn.edu.ruc.iir.paraflow.commons.exceptions.DatabaseNotFoundException;
//import cn.edu.ruc.iir.paraflow.commons.exceptions.FiberFuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
//import cn.edu.ruc.iir.paraflow.commons.exceptions.StorageFormatNotFoundException;
//import cn.edu.ruc.iir.paraflow.commons.exceptions.TableNotFoundException;
//import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.action.ActionResponse;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateDatabaseAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateUserAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetUserIdAction;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ConnectionPool;
//import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.connection.TransactionController;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
//import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConstants;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;
import io.grpc.stub.StreamObserver;

//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase
{
    private SQLTemplate SQLTemplate = new SQLTemplate();

    //TODO if ResultList is empty throw exception

//    private int findDbId(String dbName) throws DatabaseNotFoundException
//    {
//        String findDbIdSql = SQLTemplate.findDbId(dbName);
//        ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
//        if (!resFindDbId.isEmpty()) {
//            return Integer.parseInt(resFindDbId.get(0).get(0));
//        }
//        else {
//            throw new DatabaseNotFoundException(dbName);
//        }
//    }

//    private int findStorageFormatId(String storageFormatName) throws StorageFormatNotFoundException
//    {
//        String findStorageFormatIdSql = SQLTemplate.findStorageFormatId(storageFormatName);
//        ResultList resfindStorageFormatId = dbConnection.sqlQuery(findStorageFormatIdSql, 1);
//        if (!resfindStorageFormatId.isEmpty()) {
//            return Integer.parseInt(resfindStorageFormatId.get(0).get(0));
//        }
//        else {
//            throw new StorageFormatNotFoundException();
//        }
//    }

//    private String findStorageFormatName(int storageFormatId) throws StorageFormatNotFoundException
//    {
//        String findStorageFormatNameSql = SQLTemplate.findStorageFormatName(storageFormatId);
//        ResultList resfindStorageFormatName = dbConnection.sqlQuery(findStorageFormatNameSql, 1);
//        if (!resfindStorageFormatName.isEmpty()) {
//            return resfindStorageFormatName.get(0).get(0);
//        }
//        else {
//            throw new StorageFormatNotFoundException();
//        }
//    }

//    private int findFiberFuncId(String fiberColName) throws FiberFuncNotFoundException
//    {
//        String findFiberFuncIdSql = SQLTemplate.findFiberFuncId(fiberColName);
//        ResultList resfindFiberFuncId = dbConnection.sqlQuery(findFiberFuncIdSql, 1);
//        if (!resfindFiberFuncId.isEmpty()) {
//            return Integer.parseInt(resfindFiberFuncId.get(0).get(0));
//        }
//        else {
//            throw new FiberFuncNotFoundException();
//        }
//    }

//    private String findFiberFuncName(int fiberFuncId) throws FiberFuncNotFoundException
//    {
//        String findFiberFuncNameSql = SQLTemplate.findFiberFuncName(fiberFuncId);
//        ResultList resfindFiberFuncName = dbConnection.sqlQuery(findFiberFuncNameSql, 1);
//        if (!resfindFiberFuncName.isEmpty()) {
//            return resfindFiberFuncName.get(0).get(0);
//        }
//        else {
//            throw new FiberFuncNotFoundException();
//        }
//    }

//    private int findTblId(int dbId, String tblName) throws TableNotFoundException
//    {
//        String findTblIdSql = SQLTemplate.findTblId(dbId, tblName);
//        ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
//        if (!resFindTblId.isEmpty()) {
//            return Integer.parseInt(resFindTblId.get(0).get(0));
//        }
//        else {
//            throw new TableNotFoundException(tblName);
//        }
//    }

//    private String findUserName(int userId) throws UserNotFoundException
//    {
//        String findUserNameSql = SQLTemplate.findUserName(userId);
//        ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
//        if (!resFindUserName.isEmpty()) {
//            return resFindUserName.get(0).get(0);
//        }
//        else {
//            throw new UserNotFoundException();
//        }
//    }

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
        catch (ParaFlowException e)
        {
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
    public void createDatabase(MetaProto.DbParam dbParam,
                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
    {
        TransactionController txController = null;
        try {
            txController = ConnectionPool.INSTANCE().getTxController();
            ActionResponse input = new ActionResponse();
            input.setParam(dbParam);
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

//    @Override
//    public void createTable(MetaProto.TblParam tblParam,
//                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(tblParam.getDbName());
//            //find user id
//            int userId = findUserId(tblParam.getUserName());
//            String locationUrl = tblParam.getLocationUrl();
//            if (locationUrl.equals("")) {
//                metaConfig = MetaConfig.INSTANCE();
//                String url = metaConfig.getHDFSWarehouse();
//                if (url.endsWith("/")) {
//                    locationUrl = String.format("%s%s", url, tblParam.getDbName());
//                }
//                else {
//                    locationUrl = String.format("%s/%s", url, tblParam.getDbName());
//                }
//            }
//            //regular table
//            int fiberFuncId;
//            int fiberColId;
//            if (tblParam.getTblType() == 0) {
//                fiberFuncId = findFiberFuncId("none");
//                fiberColId = -1;
//            }
//            else {
//                fiberFuncId = findFiberFuncId(tblParam.getFiberFuncName());
//                fiberColId = tblParam.getFiberColId();
//            }
//            System.out.println("MetaService : fiberFuncId : " + fiberFuncId);
//            System.out.println("MetaService : fiberColId : " + fiberColId);
//            //create table
//            int storageFormatId = findStorageFormatId(tblParam.getStorageFormatName());
//            System.out.println("MetaService : storageFormatId : " + storageFormatId);
//            String createTblSql = SQLTemplate.createTable(dbId,
//                    tblParam.getTblName(), tblParam.getTblType(),
//                    userId, System.currentTimeMillis(), System.currentTimeMillis(),
//                    locationUrl, storageFormatId,
//                    fiberColId, fiberFuncId);
//            System.out.println("createTblSql : " + createTblSql);
//            int resCreateTbl = dbConnection.sqlUpdate(createTblSql);
//            System.out.println("resCreateTbl : " + resCreateTbl);
//            //result
//            if (resCreateTbl == 1) {
//                //create columns
//                //get a column param
//                MetaProto.ColListType colListType = tblParam.getColList();
//                int colCount = colListType.getColumnCount();
//                List<MetaProto.ColParam> columns = colListType.getColumnList();
//                MetaProto.ColParam colParam = columns.get(0);
//                //find table id
//                int tblId = findTblId(dbId, colParam.getTblName());
//                System.out.println("tblId : " + tblId);
//                //loop for every column to insert them
//                LinkedList<String> batchSQLs = new LinkedList<>();
//                for (int i = 0; i < colCount; i++) {
//                    MetaProto.ColParam column = columns.get(i);
//                    String sql = SQLTemplate.createColumn(i,
//                            dbId,
//                            column.getColName(),
//                            tblId,
//                            column.getColType(),
//                            column.getDataType());
//                    batchSQLs.add(sql);
//                }
//                int[] colExecute = dbConnection.sqlBatch(batchSQLs);
//                int sizeexecute = colExecute.length;
//                System.out.println("sizeexecute = " + sizeexecute);
//                for (int j = 0; j < sizeexecute; j++) {
//                    if (colExecute[0] == 0) {
//                        System.out.println("CREAT_COLUMN_ERROR");
//                        statusType = StatusProto.ResponseStatus.newBuilder()
//                                .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
//                                .build();
//                        dbConnection.rollback();
//                        responseStreamObserver.onNext(statusType);
//                        responseStreamObserver.onCompleted();
//                    }
//                }
//                //result
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                System.out.println("OK : " + statusType.getStatus());
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//                dbConnection.commit();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.TABLE_ALREADY_EXISTS)
//                        .build();
//                System.out.println("TABLE_ALREADY_EXISTS");
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (java.sql.SQLException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (UserNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (StorageFormatNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (FiberFuncNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void listDatabases(MetaProto.NoneType none,
//                              StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //query
//            String listDbSql = SQLTemplate.listDatabases();
//            ResultList resListDb = dbConnection.sqlQuery(listDbSql, 1);
//            MetaProto.StringListType stringList;
//            if (!resListDb.isEmpty()) {
//                //result
//                ArrayList<String> result = new ArrayList<>();
//                int size = resListDb.size();
//                for (int i = 0; i < size; i++) {
//                    result.add(resListDb.get(i).get(0));
//                }
//                stringList = MetaProto.StringListType.newBuilder()
//                        .addAllStr(result)
//                        .setIsEmpty(false)
//                        .build();
//                responseStreamObserver.onNext(stringList);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                stringList = MetaProto.StringListType.newBuilder().setIsEmpty(true).build();
//                responseStreamObserver.onNext(stringList);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void listTables(MetaProto.DbNameParam dbNameParam,
//                           StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//        dbConnection.autoCommitTrue();
//        try {
//            MetaProto.StringListType stringList;
//            //find database id
//            int dbId = findDbId(dbNameParam.getDatabase());
//            //query
//            String listTblSql = SQLTemplate.listTables(dbId);
//            ResultList resListTbl = dbConnection.sqlQuery(listTblSql, 1);
//            if (!resListTbl.isEmpty()) {
//                //result
//                ArrayList<String> result = new ArrayList<>();
//                int size = resListTbl.size();
//                for (int i = 0; i < size; i++) {
//                    result.add(resListTbl.get(i).get(0));
//                }
//                stringList = MetaProto.StringListType.newBuilder()
//                        .addAllStr(result)
//                        .setIsEmpty(false)
//                        .build();
//                responseStreamObserver.onNext(stringList);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                stringList = MetaProto.StringListType.newBuilder()
//                        .setIsEmpty(true)
//                        .build();
//                responseStreamObserver.onNext(stringList);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void getDatabase(MetaProto.DbNameParam dbNameParam,
//                            StreamObserver<MetaProto.DbParam> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //query
//            String getDbSql = SQLTemplate.getDatabase(dbNameParam.getDatabase());
//            ResultList resGetDb = dbConnection.sqlQuery(getDbSql, 3);
//            MetaProto.DbParam dbParam;
//            if (!resGetDb.isEmpty()) {
//                //result
//                //find user name
//                String findUserNameSql = SQLTemplate.findUserName(Integer.parseInt(resGetDb.get(0).get(2)));
//                ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
//                String userName = resFindUserName.get(0).get(0);
//                dbParam = MetaProto.DbParam.newBuilder()
//                        .setDbName(resGetDb.get(0).get(0))
//                        .setLocationUrl(resGetDb.get(0).get(1))
//                        .setUserName(userName)
//                        .setIsEmpty(false)
//                        .build();
//                responseStreamObserver.onNext(dbParam);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                dbParam = MetaProto.DbParam.newBuilder()
//                        .setIsEmpty(true)
//                        .build();
//                responseStreamObserver.onNext(dbParam);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void getTable(MetaProto.DbTblParam dbTblParam,
//                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //find database id
//            int dbId = findDbId(dbTblParam.getDatabase().getDatabase());
//            //query
//            String getTblSql = SQLTemplate.getTable(dbId, dbTblParam.getTable().getTable());
//            ResultList resGetTbl = dbConnection.sqlQuery(getTblSql, 8);
//            MetaProto.TblParam tblParam;
//            if (!resGetTbl.isEmpty()) {
//                //find username
//                String userName = findUserName(Integer.parseInt(resGetTbl.get(0).get(1)));
//                //find storageFormatName
//                String storageFormatName = findStorageFormatName(Integer.parseInt(resGetTbl.get(0).get(5)));
//                //find fiberFuncName
//                String fiberFuncName = findFiberFuncName(Integer.parseInt(resGetTbl.get(0).get(7)));
//                //result
//                tblParam = MetaProto.TblParam.newBuilder()
//                        .setDbName(dbTblParam.getDatabase().getDatabase())
//                        .setTblName(dbTblParam.getTable().getTable())
//                        .setTblType(Integer.parseInt(resGetTbl.get(0).get(0)))
//                        .setUserName(userName)
//                        .setCreateTime(Long.parseLong(resGetTbl.get(0).get(2)))
//                        .setLastAccessTime(Long.parseLong(resGetTbl.get(0).get(3)))
//                        .setLocationUrl(resGetTbl.get(0).get(4))
//                        .setStorageFormatName(storageFormatName)
//                        .setFiberColId(Integer.parseInt(resGetTbl.get(0).get(6)))
//                        .setFiberFuncName(fiberFuncName)
//                        .setIsEmpty(false)
//                        .build();
//                responseStreamObserver.onNext(tblParam);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                tblParam = MetaProto.TblParam.newBuilder()
//                        .setIsEmpty(true)
//                        .build();
//                responseStreamObserver.onNext(tblParam);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (UserNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (StorageFormatNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (FiberFuncNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void getColumn(MetaProto.DbTblColParam dbTblColParam,
//                          StreamObserver<MetaProto.ColParam> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //find database id
//            int dbId = findDbId(dbTblColParam.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, dbTblColParam.getTable().getTable());
//            //query
//            String getColSql = SQLTemplate.getColumn(tblId, dbTblColParam.getColumn().getColumn());
//            ResultList resGetCol = dbConnection.sqlQuery(getColSql, 3);
//            //result
//            MetaProto.ColParam column;
//            if (!resGetCol.isEmpty()) {
//                column = MetaProto.ColParam.newBuilder()
//                        .setColIndex(Integer.parseInt(resGetCol.get(0).get(0)))
//                        .setTblName(dbTblColParam.getTable().getTable())
//                        .setColName(dbTblColParam.getColumn().getColumn())
//                        .setColType(resGetCol.get(0).get(1))
//                        .setDataType(resGetCol.get(0).get(2))
//                        .setIsEmpty(false)
//                        .build();
//                responseStreamObserver.onNext(column);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                column = MetaProto.ColParam.newBuilder()
//                        .setIsEmpty(true)
//                        .build();
//                responseStreamObserver.onNext(column);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void renameColumn(MetaProto.RenameColParam renameColumn,
//                             StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(renameColumn.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, renameColumn.getTable().getTable());
//            //rename column
//            String renameColSql = SQLTemplate.renameColumn(dbId,
//                    tblId,
//                    renameColumn.getOldName(),
//                    renameColumn.getNewName());
//            int resRenameCol = dbConnection.sqlUpdate(renameColSql);
//            //result
//            if (resRenameCol == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.DATABASE_ALREADY_EXISTS)
//                        .build();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void renameTable(MetaProto.RenameTblParam renameTblParam,
//                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(renameTblParam.getDatabase().getDatabase());
//            //rename table
//            String renameTblSql = SQLTemplate.renameTable(dbId,
//                    renameTblParam.getOldName(),
//                    renameTblParam.getNewName());
//            int resRenameTbl = dbConnection.sqlUpdate(renameTblSql);
//            //result
//            if (resRenameTbl == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.DATABASE_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void renameDatabase(MetaProto.RenameDbParam renameDbParam,
//                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            //rename database
//            String renameDbSql = SQLTemplate.renameDatabase(
//                    renameDbParam.getOldName(),
//                    renameDbParam.getNewName());
//            int resRenameDb = dbConnection.sqlUpdate(renameDbSql);
//            //result
//            StatusProto.ResponseStatus statusType;
//            if (resRenameDb == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.DATABASE_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        dbConnection.autoCommitTrue();
//    }

//    private void deleteTblParam(int tblId)
//    {
//        //find paramKey
//        String findTblParamKeySql = SQLTemplate.findTblParamKey(tblId);
//        ResultList resFindTblParamKey = dbConnection.sqlQuery(findTblParamKeySql, 1);
//        if (!resFindTblParamKey.isEmpty()) {
//            String deleteTblParamSql = SQLTemplate.deleteTblParam(tblId);
//            dbConnection.sqlUpdate(deleteTblParamSql);
//        }
//    }

//    private void deleteTblPriv(int tblId)
//    {
//        //find TblPriv
//        String findTblPrivSql = SQLTemplate.findTblPriv(tblId);
//        ResultList resFindTblPriv = dbConnection.sqlQuery(findTblPrivSql, 1);
//        if (!resFindTblPriv.isEmpty()) {
//            String deleteTblPrivSql = SQLTemplate.deleteTblPriv(tblId);
//            dbConnection.sqlUpdate(deleteTblPrivSql);
//        }
//    }

//    private void deleteBlockIndex(int tblId)
//    {
//        //find BlockIndex
//        String findBlockIndexSql = SQLTemplate.findBlockIndex(tblId);
//        ResultList resFindBlockIndex = dbConnection.sqlQuery(findBlockIndexSql, 1);
//        if (!resFindBlockIndex.isEmpty()) {
//            String deleteBlockIndexSql = SQLTemplate.deleteBlockIndex(tblId);
//            dbConnection.sqlUpdate(deleteBlockIndexSql);
//        }
//    }

//    private void deleteDbParam(int dbId)
//    {
//        //find paramKey
//        String findDbParamKeySql = SQLTemplate.findDbParamKey(dbId);
//        ResultList resFindDbParamKey = dbConnection.sqlQuery(findDbParamKeySql, 1);
//        if (!resFindDbParamKey.isEmpty()) {
//            String deleteDbParamSql = SQLTemplate.deleteDbParam(dbId);
//            dbConnection.sqlUpdate(deleteDbParamSql);
//        }
//    }

//    @Override
//    public void deleteTable(MetaProto.DbTblParam dbTblParam,
//                            StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(dbTblParam.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, dbTblParam.getTable().getTable());
//            //delete columns
//            String deleteColSql = SQLTemplate.deleteTblColumn(dbId, tblId);
//            int resDeleteCol = dbConnection.sqlUpdate(deleteColSql);
//            deleteTblParam(tblId);
//            deleteTblPriv(tblId);
//            deleteBlockIndex(tblId);
//            //result
//            if (resDeleteCol != 0) {
//                //delete table
//                String deleteTblSql = SQLTemplate.deleteTable(dbId, dbTblParam.getTable().getTable());
//                int resDeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
//                //result
//                if (resDeleteTbl == 1) {
//                    statusType = StatusProto.ResponseStatus.newBuilder()
//                            .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                            .build();
//                    dbConnection.commit();
//                    responseStreamObserver.onNext(statusType);
//                    responseStreamObserver.onCompleted();
//                }
//                else {
//                    statusType = StatusProto.ResponseStatus.newBuilder()
//                            .setStatus(StatusProto.ResponseStatus.State.DELETE_TABLE_ERROR)
//                            .build();
//                    dbConnection.rollback();
//                    responseStreamObserver.onNext(statusType);
//                    responseStreamObserver.onCompleted();
//                }
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.DELETE_COLUMN_ERROR)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void deleteDatabase(MetaProto.DbNameParam dbNameParam,
//                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(dbNameParam.getDatabase());
//            //find table
//            String findTblSql = SQLTemplate.findTblIdWithoutName(dbId);
//            ResultList resFindTbl = dbConnection.sqlQuery(findTblSql, 1);
//            if (!resFindTbl.isEmpty()) {
//                int size = resFindTbl.size();
//                for (int i = 0; i < size; i++) {
//                    deleteTblParam(Integer.parseInt(resFindTbl.get(i).get(0)));
//                    deleteTblPriv(Integer.parseInt(resFindTbl.get(i).get(0)));
//                    deleteBlockIndex(Integer.parseInt(resFindTbl.get(i).get(0)));
//                }
//                String deleteColSql = SQLTemplate.deleteDbColumn(dbId);
//                dbConnection.sqlUpdate(deleteColSql);
//                String deleteTblSql = SQLTemplate.deleteDbTable(dbId);
//                dbConnection.sqlUpdate(deleteTblSql);
//                deleteDbParam(dbId);
//            }
//            String deleteDbSql = SQLTemplate.deleteDatabase(dbNameParam.getDatabase());
//            dbConnection.sqlUpdate(deleteDbSql);
//            dbConnection.commit();
//            statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//            responseStreamObserver.onNext(statusType);
//            responseStreamObserver.onCompleted();
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createDbParam(MetaProto.DbParamParam dbParam,
//                              StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(dbParam.getDbName());
//            //create database param
//            String createDbParamSql = SQLTemplate.createDbParam(
//                    dbId,
//                    dbParam.getParamKey(),
//                    dbParam.getParamValue());
//            int resCreateDbParam = dbConnection.sqlUpdate(createDbParamSql);
//            //result
//            if (resCreateDbParam == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.DATABASE_PARAM_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createTblParam(MetaProto.TblParamParam tblParam,
//                               StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(tblParam.getDbName());
//            //find table id
//            int tblId = findTblId(dbId, tblParam.getTblName());
//            //create table param
//            String createTblParamSql = SQLTemplate.createTblParam(
//                    tblId,
//                    tblParam.getParamKey(),
//                    tblParam.getParamValue());
//            int resCreateTblParam = dbConnection.sqlUpdate(createTblParamSql);
//            //result
//            if (resCreateTblParam == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.TABLE_PARAM_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createTblPriv(MetaProto.TblPrivParam tblPriv,
//                              StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(tblPriv.getDbName());
//            //find table id
//            int tblId = findTblId(dbId, tblPriv.getTblName());
//            //find user id
//            int userId = findUserId(tblPriv.getUserName());
//            //create table priv
//            String createTblPrivSql = SQLTemplate.createTblPriv(
//                    tblId,
//                    userId,
//                    tblPriv.getPrivType(),
//                    System.currentTimeMillis());
//            int resCreateTblPriv = dbConnection.sqlUpdate(createTblPrivSql);
//            //result
//            if (resCreateTblPriv == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.TABLE_PRIV_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (UserNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createStorageFormat(MetaProto.StorageFormatParam storageFormat,
//                                    StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            //create storage format
//            String createStorageFormatSql = SQLTemplate.createStorageFormat(
//                    storageFormat.getStorageFormatName(),
//                    storageFormat.getCompression(),
//                    storageFormat.getSerialFormat());
//            int resCreateStorageFormat = dbConnection.sqlUpdate(createStorageFormatSql);
//            //result
//            StatusProto.ResponseStatus statusType;
//            if (resCreateStorageFormat == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STORAGE_FORMAT_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createFiberFunc(MetaProto.FiberFuncParam fiberFunc,
//                                StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            //create storage format
//            String createFiberFuncSql = SQLTemplate.createFiberFunc(
//                    fiberFunc.getFiberFuncName(),
//                    fiberFunc.getFiberFuncContent());
//            int resCreateFiberFunc = dbConnection.sqlUpdate(createFiberFuncSql);
//            //result
//            StatusProto.ResponseStatus statusType;
//            if (resCreateFiberFunc == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.FIBER_FUNCTION_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void createBlockIndex(MetaProto.BlockIndexParam blockIndex,
//                                 StreamObserver<StatusProto.ResponseStatus> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitFalse();
//            StatusProto.ResponseStatus statusType;
//            //find database id
//            int dbId = findDbId(blockIndex.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, blockIndex.getTable().getTable());
//            //create storage format
//            String createBlockIndexSql = SQLTemplate.createBlockIndex(
//                    tblId,
//                    blockIndex.getValue().getValue(),
//                    blockIndex.getTimeBegin(),
//                    blockIndex.getTimeEnd(),
//                    blockIndex.getTimeZone(),
//                    blockIndex.getBlockPath());
//            int rescreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
//            //result
//            if (rescreateBlockIndex == 1) {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
//                        .build();
//                dbConnection.commit();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = StatusProto.ResponseStatus.newBuilder()
//                        .setStatus(StatusProto.ResponseStatus.State.BLOCK_INDEX_ALREADY_EXISTS)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitTrue();
//    }

//    @Override
//    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockIndexParam,
//                                 StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //find database id
//            int dbId = findDbId(filterBlockIndexParam.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, filterBlockIndexParam.getTable().getTable());
//            ArrayList<String> result = new ArrayList<>();
//            String filterBlockIndexSql;
//            if (filterBlockIndexParam.getTimeBegin() == -1
//                    && filterBlockIndexParam.getTimeEnd() == -1) {
//                //query
//                filterBlockIndexSql = SQLTemplate.filterBlockIndex(tblId);
//            }
//            else if (filterBlockIndexParam.getTimeBegin() == -1
//                    && filterBlockIndexParam.getTimeEnd() != -1) {
//                //query
//                filterBlockIndexSql = SQLTemplate.filterBlockIndexEnd(
//                        tblId,
//                        filterBlockIndexParam.getTimeEnd());
//            }
//            else if (filterBlockIndexParam.getTimeBegin() != -1
//                    && filterBlockIndexParam.getTimeEnd() == -1) {
//                //query
//                filterBlockIndexSql = SQLTemplate.filterBlockIndexBegin(
//                        tblId,
//                        filterBlockIndexParam.getTimeBegin());
//            }
//            else {
//                //query
//                filterBlockIndexSql = SQLTemplate.filterBlockIndexBeginEnd(
//                        tblId,
//                        filterBlockIndexParam.getTimeBegin(),
//                        filterBlockIndexParam.getTimeEnd());
//            }
//            ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
//            int size = resfilterBlockIndex.size();
//            for (int i = 0; i < size; i++) {
//                result.add(resfilterBlockIndex.get(i).get(0));
//            }
//            //result
//            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
//                    .addAllStr(result)
//                    .build();
//            responseStreamObserver.onNext(stringList);
//            responseStreamObserver.onCompleted();
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitFalse();
//    }

//    @Override
//    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam,
//                                        StreamObserver<MetaProto.StringListType> responseStreamObserver)
//    {
//        try {
//            dbConnection.autoCommitTrue();
//            //find database id
//            int dbId = findDbId(filterBlockIndexByFiberParam.getDatabase().getDatabase());
//            //find table id
//            int tblId = findTblId(dbId, filterBlockIndexByFiberParam.getTable().getTable());
//            ArrayList<String> result = new ArrayList<>();
//            String filterBlockIndexByFiberSql;
//            if (filterBlockIndexByFiberParam.getTimeBegin() == -1
//                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
//                //query
//                filterBlockIndexByFiberSql = SQLTemplate.filterBlockIndexByFiber(
//                        tblId,
//                        filterBlockIndexByFiberParam.getValue().getValue());
//            }
//            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1
//                    && filterBlockIndexByFiberParam.getTimeEnd() != -1) {
//                //query
//                filterBlockIndexByFiberSql = SQLTemplate.filterBlockIndexByFiberEnd(
//                        tblId,
//                        filterBlockIndexByFiberParam.getValue().getValue(),
//                        filterBlockIndexByFiberParam.getTimeEnd());
//            }
//            else if (filterBlockIndexByFiberParam.getTimeBegin() != -1
//                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
//                //query
//                filterBlockIndexByFiberSql = SQLTemplate.filterBlockIndexByFiberBegin(
//                        tblId,
//                        filterBlockIndexByFiberParam.getValue().getValue(),
//                        filterBlockIndexByFiberParam.getTimeBegin());
//            }
//            else {
//                //query
//                filterBlockIndexByFiberSql = SQLTemplate.filterBlockIndexByFiberBeginEnd(
//                        tblId,
//                        filterBlockIndexByFiberParam.getValue().getValue(),
//                        filterBlockIndexByFiberParam.getTimeBegin(),
//                        filterBlockIndexByFiberParam.getTimeEnd());
//            }
//            ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
//            int size = resfilterBlockIndexByFiber.size();
//            for (int i = 0; i < size; i++) {
//                result.add(resfilterBlockIndexByFiber.get(i).get(0));
//            }
//            //result
//            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
//                    .addAllStr(result)
//                    .build();
//            responseStreamObserver.onNext(stringList);
//            responseStreamObserver.onCompleted();
//        }
//        catch (NullPointerException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//        catch (DatabaseNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        catch (TableNotFoundException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        dbConnection.autoCommitFalse();
//    }

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
