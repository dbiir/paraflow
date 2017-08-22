package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.DatabaseNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.FiberFuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.StorageFormatNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.UserNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.connection.SqlGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import cn.edu.ruc.iir.paraflow.metaserver.server.MetaServer;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase
{
    private SqlGenerator sqlGenerator = new SqlGenerator();
    private DBConnection dbConnection = DBConnection.getConnectionInstance();
    MetaConfig metaConfig;

    //TODO if ResultList is empty throw exception

    private int findDbId(String dbName) throws DatabaseNotFoundException
    {
        String findDbIdSql = sqlGenerator.findDbId(dbName);
        ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
        if (!resFindDbId.isEmpty()) {
            return Integer.parseInt(resFindDbId.get(0).get(0));
        }
        else {
            throw new DatabaseNotFoundException(dbName);
        }
    }

    private int findStorageFormatId(String storageFormatName) throws StorageFormatNotFoundException
    {
        String findStorageFormatIdSql = sqlGenerator.findStorageFormatId(storageFormatName);
        ResultList resfindStorageFormatId = dbConnection.sqlQuery(findStorageFormatIdSql, 1);
        if (!resfindStorageFormatId.isEmpty()) {
            return Integer.parseInt(resfindStorageFormatId.get(0).get(0));
        }
        else {
            throw new StorageFormatNotFoundException();
        }
    }

    private String findStorageFormatName(int storageFormatId) throws StorageFormatNotFoundException
    {
        String findStorageFormatNameSql = sqlGenerator.findStorageFormatName(storageFormatId);
        ResultList resfindStorageFormatName = dbConnection.sqlQuery(findStorageFormatNameSql, 1);
        if (!resfindStorageFormatName.isEmpty()) {
            return resfindStorageFormatName.get(0).get(0);
        }
        else {
            throw new StorageFormatNotFoundException();
        }
    }

    private int findFiberFuncId(String fiberColName) throws FiberFuncNotFoundException
    {
        String findFiberFuncIdSql = sqlGenerator.findFiberFuncId(fiberColName);
        ResultList resfindFiberFuncId = dbConnection.sqlQuery(findFiberFuncIdSql, 1);
        if (!resfindFiberFuncId.isEmpty()) {
            return Integer.parseInt(resfindFiberFuncId.get(0).get(0));
        }
        else {
            throw new FiberFuncNotFoundException();
        }
    }

    private String findFiberFuncName(int fiberFuncId) throws FiberFuncNotFoundException
    {
        String findFiberFuncNameSql = sqlGenerator.findFiberFuncName(fiberFuncId);
        ResultList resfindFiberFuncName = dbConnection.sqlQuery(findFiberFuncNameSql, 1);
        if (!resfindFiberFuncName.isEmpty()) {
            return resfindFiberFuncName.get(0).get(0);
        }
        else {
            throw new FiberFuncNotFoundException();
        }
    }

    private int findUserId(String userName) throws UserNotFoundException
    {
        String findUserIdSql = sqlGenerator.findUserId(userName);
        ResultList resFindUserId = dbConnection.sqlQuery(findUserIdSql, 1);
        if (!resFindUserId.isEmpty()) {
            return Integer.parseInt(resFindUserId.get(0).get(0));
        }
        else {
            throw new UserNotFoundException();
        }
    }

    private int findTblId(int dbId, String tblName) throws TableNotFoundException
    {
        String findTblIdSql = sqlGenerator.findTblId(dbId, tblName);
        ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
        if (!resFindTblId.isEmpty()) {
            return Integer.parseInt(resFindTblId.get(0).get(0));
        }
        else {
            throw new TableNotFoundException(tblName);
        }
    }

    private String findUserName(int userId) throws UserNotFoundException
    {
        String findUserNameSql = sqlGenerator.findUserName(userId);
        ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
        if (!resFindUserName.isEmpty()) {
            return resFindUserName.get(0).get(0);
        }
        else {
            throw new UserNotFoundException();
        }
    }

    @Override
    public void createUser(MetaProto.UserParam user,
                           StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            //create user
            String createUserSql = sqlGenerator.createUser(
                    user.getUserName(),
                    user.getPassword(),
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
            System.out.println(createUserSql);
            int resCreateUser = dbConnection.sqlUpdate(createUserSql);
            System.out.println(resCreateUser);
            //result
            MetaProto.StatusType statusType;
            if (resCreateUser == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.USER_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createDatabase(MetaProto.DbParam dbParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find user id
            int userId = findUserId(dbParam.getUserName());
            //create database
            String locationUrl = dbParam.getLocationUrl();
            if (locationUrl.equals("")) {
                metaConfig = new MetaConfig();
                String url = metaConfig.getHDFSWarehouse();
                if (url.endsWith("/")) {
                    locationUrl = String.format("%s%s", url, dbParam.getDbName());
                }
                else {
                    locationUrl = String.format("%s/%s", url, dbParam.getDbName());
                }
            }
            else {
                locationUrl = dbParam.getLocationUrl();
            }
            String createDbSql = sqlGenerator.createDatabase(dbParam.getDbName(),
                    userId,
                    locationUrl);
            int resCreateDb = dbConnection.sqlUpdate(createDbSql);
            //result
            if (resCreateDb == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (ConfigFileNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (UserNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.USER_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createTable(MetaProto.TblParam tblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(tblParam.getDbName());
            System.out.println("********dbId : dbId : dbId : dbId :" + dbId);
            //find user id
            int userId = findUserId(tblParam.getUserName());
            System.out.println("********userId : userId : userId : userId : " + userId);
            String locationUrl = tblParam.getLocationUrl();
            System.out.println("********locationUrl : locationUrl : locationUrl : locationUrl : " + locationUrl);
            if (locationUrl.equals("")) {
                metaConfig = new MetaConfig();
                String url = metaConfig.getHDFSWarehouse();
                if (url.endsWith("/")) {
                    locationUrl = String.format("%s%s/%s", url, tblParam.getDbName(), tblParam.getTblName());
                }
                else {
                    locationUrl = String.format("%s/%s/%s", url, tblParam.getDbName(), tblParam.getTblName());
                }
            }
            System.out.println("********locationUrl : locationUrl : locationUrl : locationUrl : " + locationUrl);
            //regular table
            int fiberFuncId;
            int fiberColId;
            if (tblParam.getTblType() == 0) {
                fiberFuncId = findFiberFuncId("none");
                fiberColId = -1;
            }
            else {
                fiberFuncId = findFiberFuncId(tblParam.getFiberFuncName());
                fiberColId = tblParam.getFiberColId();
            }
            System.out.println("********MetaService : fiberFuncId : " + fiberFuncId);
            System.out.println("********MetaService : fiberColId : " + fiberColId);
            //create table
            int storageFormatId = findStorageFormatId(tblParam.getStorageFormatName());
            System.out.println("********MetaService : storageFormatId : " + storageFormatId);
            String createTblSql = sqlGenerator.createTable(dbId,
                    tblParam.getTblName(), tblParam.getTblType(),
                    userId, System.currentTimeMillis(), System.currentTimeMillis(),
                    locationUrl, storageFormatId,
                    fiberColId, fiberFuncId);
            System.out.println("********createTblSql : " + createTblSql);
            int resCreateTbl = dbConnection.sqlUpdate(createTblSql);
            System.out.println("********resCreateTbl : " + resCreateTbl);
            //result
            if (resCreateTbl == 1) {
                //create columns
                //get a column param
                MetaProto.ColListType colListType = tblParam.getColList();
                int colCount = colListType.getColumnCount();
                List<MetaProto.ColParam> columns = colListType.getColumnList();
                MetaProto.ColParam colParam = columns.get(0);
                //find table id
                int tblId = findTblId(dbId, colParam.getTblName());
                System.out.println("********tblId : " + tblId);
                //loop for every column to insert them
                LinkedList<String> batchSQLs = new LinkedList<>();
                for (int i = 0; i < colCount; i++) {
                    MetaProto.ColParam column = columns.get(i);
                    String sql = sqlGenerator.createColumn(i,
                            dbId,
                            column.getColName(),
                            tblId,
                            column.getColType(),
                            column.getDataType());
                    batchSQLs.add(sql);
                }
                int[] colExecute = dbConnection.sqlBatch(batchSQLs);
                int sizeexecute = colExecute.length;
                System.out.println("********sizeexecute = " + sizeexecute);
                for (int j = 0; j < sizeexecute; j++) {
                    if (colExecute[0] == 0) {
                        System.out.println("******CREATE_COLUMN_ERROR");
                        statusType = MetaProto.StatusType.newBuilder()
                                .setStatus(MetaProto.StatusType.State.CREATE_COLUMN_ERROR)
                                .build();
                        dbConnection.rollback();
                        responseStreamObserver.onNext(statusType);
                        responseStreamObserver.onCompleted();
                    }
                }
                //result
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                System.out.println("********OK : " + statusType.getStatus());
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
                dbConnection.commit();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS)
                        .build();
                System.out.println("******TABLE_ALREADY_EXISTS");
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (ConfigFileNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusDbNf = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusDbNf);
            responseStreamObserver.onCompleted();
        }
        catch (UserNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.USER_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (StorageFormatNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.STORAGE_FORMAT_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (FiberFuncNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.FIBER_FUNCTION_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void listDatabases(MetaProto.NoneType none,
                              StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //query
            String listDbSql = sqlGenerator.listDatabases();
            ResultList resListDb = dbConnection.sqlQuery(listDbSql, 1);
            MetaProto.StringListType stringList;
            if (!resListDb.isEmpty()) {
                //result
                ArrayList<String> result = new ArrayList<>();
                int size = resListDb.size();
                for (int i = 0; i < size; i++) {
                    result.add(resListDb.get(i).get(0));
                }
                stringList = MetaProto.StringListType.newBuilder()
                        .addAllStr(result)
                        .setIsEmpty(false)
                        .build();
                responseStreamObserver.onNext(stringList);
                responseStreamObserver.onCompleted();
            }
            else {
                stringList = MetaProto.StringListType.newBuilder().setIsEmpty(true).build();
                responseStreamObserver.onNext(stringList);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam,
                           StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        dbConnection.autoCommitTrue();
        try {
            MetaProto.StringListType stringList;
            //find database id
            int dbId = findDbId(dbNameParam.getDatabase());
            //query
            String listTblSql = sqlGenerator.listTables(dbId);
            ResultList resListTbl = dbConnection.sqlQuery(listTblSql, 1);
            if (!resListTbl.isEmpty()) {
                //result
                ArrayList<String> result = new ArrayList<>();
                int size = resListTbl.size();
                for (int i = 0; i < size; i++) {
                    result.add(resListTbl.get(i).get(0));
                }
                stringList = MetaProto.StringListType.newBuilder()
                        .addAllStr(result)
                        .setIsEmpty(false)
                        .build();
                responseStreamObserver.onNext(stringList);
                responseStreamObserver.onCompleted();
            }
            else {
                stringList = MetaProto.StringListType.newBuilder()
                        .setIsEmpty(true)
                        .build();
                responseStreamObserver.onNext(stringList);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam,
                            StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //query
            String getDbSql = sqlGenerator.getDatabase(dbNameParam.getDatabase());
            ResultList resGetDb = dbConnection.sqlQuery(getDbSql, 3);
            MetaProto.DbParam dbParam;
            if (!resGetDb.isEmpty()) {
                //result
                //find user name
                String findUserNameSql = sqlGenerator.findUserName(Integer.parseInt(resGetDb.get(0).get(2)));
                ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
                String userName = resFindUserName.get(0).get(0);
                dbParam = MetaProto.DbParam.newBuilder()
                        .setDbName(resGetDb.get(0).get(0))
                        .setLocationUrl(resGetDb.get(0).get(1))
                        .setUserName(userName)
                        .setIsEmpty(false)
                        .build();
                responseStreamObserver.onNext(dbParam);
                responseStreamObserver.onCompleted();
            }
            else {
                dbParam = MetaProto.DbParam.newBuilder()
                        .setIsEmpty(true)
                        .build();
                responseStreamObserver.onNext(dbParam);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam,
                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //find database id
            int dbId = findDbId(dbTblParam.getDatabase().getDatabase());
            //query
            String getTblSql = sqlGenerator.getTable(dbId, dbTblParam.getTable().getTable());
            ResultList resGetTbl = dbConnection.sqlQuery(getTblSql, 8);
            MetaProto.TblParam tblParam;
            if (!resGetTbl.isEmpty()) {
                //find username
                String userName = findUserName(Integer.parseInt(resGetTbl.get(0).get(1)));
                //find storageFormatName
                String storageFormatName = findStorageFormatName(Integer.parseInt(resGetTbl.get(0).get(5)));
                //find fiberFuncName
                String fiberFuncName = findFiberFuncName(Integer.parseInt(resGetTbl.get(0).get(7)));
                //result
                tblParam = MetaProto.TblParam.newBuilder()
                        .setDbName(dbTblParam.getDatabase().getDatabase())
                        .setTblName(dbTblParam.getTable().getTable())
                        .setTblType(Integer.parseInt(resGetTbl.get(0).get(0)))
                        .setUserName(userName)
                        .setCreateTime(Long.parseLong(resGetTbl.get(0).get(2)))
                        .setLastAccessTime(Long.parseLong(resGetTbl.get(0).get(3)))
                        .setLocationUrl(resGetTbl.get(0).get(4))
                        .setStorageFormatName(storageFormatName)
                        .setFiberColId(Integer.parseInt(resGetTbl.get(0).get(6)))
                        .setFiberFuncName(fiberFuncName)
                        .setIsEmpty(false)
                        .build();
                responseStreamObserver.onNext(tblParam);
                responseStreamObserver.onCompleted();
            }
            else {
                tblParam = MetaProto.TblParam.newBuilder()
                        .setIsEmpty(true)
                        .build();
                responseStreamObserver.onNext(tblParam);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (UserNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (StorageFormatNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (FiberFuncNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam,
                          StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //find database id
            int dbId = findDbId(dbTblColParam.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, dbTblColParam.getTable().getTable());
            //query
            String getColSql = sqlGenerator.getColumn(tblId, dbTblColParam.getColumn().getColumn());
            ResultList resGetCol = dbConnection.sqlQuery(getColSql, 3);
            //result
            MetaProto.ColParam column;
            if (!resGetCol.isEmpty()) {
                column = MetaProto.ColParam.newBuilder()
                        .setColIndex(Integer.parseInt(resGetCol.get(0).get(0)))
                        .setTblName(dbTblColParam.getTable().getTable())
                        .setColName(dbTblColParam.getColumn().getColumn())
                        .setColType(resGetCol.get(0).get(1))
                        .setDataType(resGetCol.get(0).get(2))
                        .setIsEmpty(false)
                        .build();
                responseStreamObserver.onNext(column);
                responseStreamObserver.onCompleted();
            }
            else {
                column = MetaProto.ColParam.newBuilder()
                        .setIsEmpty(true)
                        .build();
                responseStreamObserver.onNext(column);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.ColParam colParam = MetaProto.ColParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.ColParam colParam = MetaProto.ColParam.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(colParam);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn,
                             StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(renameColumn.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, renameColumn.getTable().getTable());
            //rename column
            String renameColSql = sqlGenerator.renameColumn(dbId,
                    tblId,
                    renameColumn.getOldName(),
                    renameColumn.getNewName());
            int resRenameCol = dbConnection.sqlUpdate(renameColSql);
            //result
            if (resRenameCol == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(renameTblParam.getDatabase().getDatabase());
            //rename table
            String renameTblSql = sqlGenerator.renameTable(dbId,
                    renameTblParam.getOldName(),
                    renameTblParam.getNewName());
            int resRenameTbl = dbConnection.sqlUpdate(renameTblSql);
            //result
            if (resRenameTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            //rename database
            String renameDbSql = sqlGenerator.renameDatabase(
                    renameDbParam.getOldName(),
                    renameDbParam.getNewName());
            int resRenameDb = dbConnection.sqlUpdate(renameDbSql);
            //result
            MetaProto.StatusType statusType;
            if (resRenameDb == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitTrue();
    }

    private void deleteTblParam(int tblId)
    {
        //find paramKey
        String findTblParamKeySql = sqlGenerator.findTblParamKey(tblId);
        ResultList resFindTblParamKey = dbConnection.sqlQuery(findTblParamKeySql, 1);
        if (!resFindTblParamKey.isEmpty()) {
            String deleteTblParamSql = sqlGenerator.deleteTblParam(tblId);
            dbConnection.sqlUpdate(deleteTblParamSql);
        }
    }

    private void deleteTblPriv(int tblId)
    {
        //find TblPriv
        String findTblPrivSql = sqlGenerator.findTblPriv(tblId);
        ResultList resFindTblPriv = dbConnection.sqlQuery(findTblPrivSql, 1);
        if (!resFindTblPriv.isEmpty()) {
            String deleteTblPrivSql = sqlGenerator.deleteTblPriv(tblId);
            dbConnection.sqlUpdate(deleteTblPrivSql);
        }
    }

    private void deleteBlockIndex(int tblId)
    {
        //find BlockIndex
        String findBlockIndexSql = sqlGenerator.findBlockIndex(tblId);
        ResultList resFindBlockIndex = dbConnection.sqlQuery(findBlockIndexSql, 1);
        if (!resFindBlockIndex.isEmpty()) {
            String deleteBlockIndexSql = sqlGenerator.deleteBlockIndex(tblId);
            dbConnection.sqlUpdate(deleteBlockIndexSql);
        }
    }

    private void deleteDbParam(int dbId)
    {
        //find paramKey
        String findDbParamKeySql = sqlGenerator.findDbParamKey(dbId);
        ResultList resFindDbParamKey = dbConnection.sqlQuery(findDbParamKeySql, 1);
        if (!resFindDbParamKey.isEmpty()) {
            String deleteDbParamSql = sqlGenerator.deleteDbParam(dbId);
            dbConnection.sqlUpdate(deleteDbParamSql);
        }
    }

    @Override
    public void deleteTable(MetaProto.DbTblParam dbTblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(dbTblParam.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, dbTblParam.getTable().getTable());
            //delete columns
            String deleteColSql = sqlGenerator.deleteTblColumn(dbId, tblId);
            int resDeleteCol = dbConnection.sqlUpdate(deleteColSql);
            deleteTblParam(tblId);
            deleteTblPriv(tblId);
            deleteBlockIndex(tblId);
            //result
            if (resDeleteCol != 0) {
                //delete table
                String deleteTblSql = sqlGenerator.deleteTable(dbId, dbTblParam.getTable().getTable());
                int resDeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
                //result
                if (resDeleteTbl == 1) {
                    statusType = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.OK)
                            .build();
                    dbConnection.commit();
                    responseStreamObserver.onNext(statusType);
                    responseStreamObserver.onCompleted();
                }
                else {
                    statusType = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.DELETE_TABLE_ERROR)
                            .build();
                    dbConnection.rollback();
                    responseStreamObserver.onNext(statusType);
                    responseStreamObserver.onCompleted();
                }
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DELETE_COLUMN_ERROR)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam dbNameParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(dbNameParam.getDatabase());
            //find table
            String findTblSql = sqlGenerator.findTblIdWithoutName(dbId);
            ResultList resFindTbl = dbConnection.sqlQuery(findTblSql, 1);
            if (!resFindTbl.isEmpty()) {
                int size = resFindTbl.size();
                for (int i = 0; i < size; i++) {
                    deleteTblParam(Integer.parseInt(resFindTbl.get(i).get(0)));
                    deleteTblPriv(Integer.parseInt(resFindTbl.get(i).get(0)));
                    deleteBlockIndex(Integer.parseInt(resFindTbl.get(i).get(0)));
                }
                String deleteColSql = sqlGenerator.deleteDbColumn(dbId);
                dbConnection.sqlUpdate(deleteColSql);
                String deleteTblSql = sqlGenerator.deleteDbTable(dbId);
                dbConnection.sqlUpdate(deleteTblSql);
                deleteDbParam(dbId);
            }
            String deleteDbSql = sqlGenerator.deleteDatabase(dbNameParam.getDatabase());
            dbConnection.sqlUpdate(deleteDbSql);
            dbConnection.commit();
            statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createDbParam(MetaProto.DbParamParam dbParam,
                              StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(dbParam.getDbName());
            //create database param
            String createDbParamSql = sqlGenerator.createDbParam(
                    dbId,
                    dbParam.getParamKey(),
                    dbParam.getParamValue());
            int resCreateDbParam = dbConnection.sqlUpdate(createDbParamSql);
            //result
            if (resCreateDbParam == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_PARAM_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createTblParam(MetaProto.TblParamParam tblParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(tblParam.getDbName());
            //find table id
            int tblId = findTblId(dbId, tblParam.getTblName());
            //create table param
            String createTblParamSql = sqlGenerator.createTblParam(
                    tblId,
                    tblParam.getParamKey(),
                    tblParam.getParamValue());
            int resCreateTblParam = dbConnection.sqlUpdate(createTblParamSql);
            //result
            if (resCreateTblParam == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_PARAM_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createTblPriv(MetaProto.TblPrivParam tblPriv,
                              StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(tblPriv.getDbName());
            //find table id
            int tblId = findTblId(dbId, tblPriv.getTblName());
            //find user id
            int userId = findUserId(tblPriv.getUserName());
            //create table priv
            String createTblPrivSql = sqlGenerator.createTblPriv(
                    tblId,
                    userId,
                    tblPriv.getPrivType(),
                    System.currentTimeMillis());
            int resCreateTblPriv = dbConnection.sqlUpdate(createTblPrivSql);
            //result
            if (resCreateTblPriv == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_PRIV_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (UserNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.USER_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createStorageFormat(MetaProto.StorageFormatParam storageFormat,
                                    StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            //create storage format
            String createStorageFormatSql = sqlGenerator.createStorageFormat(
                    storageFormat.getStorageFormatName(),
                    storageFormat.getCompression(),
                    storageFormat.getSerialFormat());
            int resCreateStorageFormat = dbConnection.sqlUpdate(createStorageFormatSql);
            //result
            MetaProto.StatusType statusType;
            if (resCreateStorageFormat == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.STORAGE_FORMAT_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createFiberFunc(MetaProto.FiberFuncParam fiberFunc,
                                StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            //create storage format
            String createFiberFuncSql = sqlGenerator.createFiberFunc(
                    fiberFunc.getFiberFuncName(),
                    fiberFunc.getFiberFuncContent());
            int resCreateFiberFunc = dbConnection.sqlUpdate(createFiberFuncSql);
            //result
            MetaProto.StatusType statusType;
            if (resCreateFiberFunc == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.FIBER_FUNCTION_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void createBlockIndex(MetaProto.BlockIndexParam blockIndex,
                                 StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitFalse();
            MetaProto.StatusType statusType;
            //find database id
            int dbId = findDbId(blockIndex.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, blockIndex.getTable().getTable());
            //create storage format
            String createBlockIndexSql = sqlGenerator.createBlockIndex(
                    tblId,
                    blockIndex.getValue().getValue(),
                    blockIndex.getTimeBegin(),
                    blockIndex.getTimeEnd(),
                    blockIndex.getTimeZone(),
                    blockIndex.getBlockPath());
            int rescreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
            //result
            if (rescreateBlockIndex == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                dbConnection.commit();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.BLOCK_INDEX_ALREADY_EXISTS)
                        .build();
                dbConnection.rollback();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitTrue();
    }

    @Override
    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockIndexParam,
                                 StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //find database id
            int dbId = findDbId(filterBlockIndexParam.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, filterBlockIndexParam.getTable().getTable());
            ArrayList<String> result = new ArrayList<>();
            String filterBlockIndexSql;
            if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                filterBlockIndexSql = sqlGenerator.filterBlockIndex(tblId);
            }
            else if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() != -1) {
                //query
                filterBlockIndexSql = sqlGenerator.filterBlockIndexEnd(
                        tblId,
                        filterBlockIndexParam.getTimeEnd());
            }
            else if (filterBlockIndexParam.getTimeBegin() != -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                filterBlockIndexSql = sqlGenerator.filterBlockIndexBegin(
                        tblId,
                        filterBlockIndexParam.getTimeBegin());
            }
            else {
                //query
                filterBlockIndexSql = sqlGenerator.filterBlockIndexBeginEnd(
                        tblId,
                        filterBlockIndexParam.getTimeBegin(),
                        filterBlockIndexParam.getTimeEnd());
            }
            System.out.println("******filterBlockIndexSql : " + filterBlockIndexSql);
            ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
            int size = resfilterBlockIndex.size();
            for (int i = 0; i < size; i++) {
                result.add(resfilterBlockIndex.get(i).get(0));
            }
            //result
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .addAllStr(result)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam,
                                        StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //find database id
            int dbId = findDbId(filterBlockIndexByFiberParam.getDatabase().getDatabase());
            //find table id
            int tblId = findTblId(dbId, filterBlockIndexByFiberParam.getTable().getTable());
            ArrayList<String> result = new ArrayList<>();
            String filterBlockIndexByFiberSql;
            if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiber(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue());
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() != -1) {
                //query
                filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeEnd());
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() != -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBegin(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin());
            }
            else {
                //query
                filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBeginEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin(),
                        filterBlockIndexByFiberParam.getTimeEnd());
            }
            ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
            int size = resfilterBlockIndexByFiber.size();
            for (int i = 0; i < size; i++) {
                result.add(resfilterBlockIndexByFiber.get(i).get(0));
            }
            //result
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder()
                    .addAllStr(result)
                    .build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (DatabaseNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        catch (TableNotFoundException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            MetaProto.StringListType stringListType = MetaProto.StringListType.newBuilder()
                    .setIsEmpty(true)
                    .build();
            dbConnection.rollback();
            responseStreamObserver.onNext(stringListType);
            responseStreamObserver.onCompleted();
        }
        dbConnection.autoCommitFalse();
    }

    @Override
    public void stopServer(MetaProto.NoneType noneType,
                           StreamObserver<MetaProto.NoneType> responseStreamObserver)
    {
        MetaServer metaServer = MetaServer.getServerInstance();
        metaServer.stop();
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        responseStreamObserver.onNext(none);
        responseStreamObserver.onCompleted();
    }
}
