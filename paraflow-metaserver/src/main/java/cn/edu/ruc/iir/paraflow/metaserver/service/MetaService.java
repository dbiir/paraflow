package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.connection.SqlGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
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

    @Override
    public void createUser(MetaProto.UserParam user,
                           StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
//            //create user
            String createUserSql = sqlGenerator.createUser(
                    user.getUserName(),
                    user.getPassword(),
                    user.getCreateTime(),
                    user.getLastVisitTime());
            int resCreateUser = dbConnection.sqlUpdate(createUserSql);
            System.out.println(resCreateUser);
            //result
            MetaProto.StatusType statusType;
            if (resCreateUser == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void createDatabase(MetaProto.DbParam dbParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(dbParam.getUserName());
            ResultList resFindUserId = dbConnection.sqlQuery(findUserIdSql, 1);
            int userId = Integer.parseInt(resFindUserId.get(0).get(0));
            MetaProto.StatusType statusType;
            //create database
            String createDbSql = sqlGenerator.createDatabase(dbParam.getDbName(),
                    userId,
                    dbParam.getLocationUrl());
            int resCreateDb = dbConnection.sqlUpdate(createDbSql);
            //result
            if (resCreateDb == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void createTable(MetaProto.TblParam tblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(tblParam.getDbName());
            System.out.println("find dbId Sql : " + findDbIdSql);
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(tblParam.getUserName());
            ResultList resFindUserId = dbConnection.sqlQuery(findUserIdSql, 1);
            int userId = Integer.parseInt(resFindUserId.get(0).get(0));
            //create table
            String createTblSql = sqlGenerator.createTable(dbId,
                    tblParam.getTblName(), tblParam.getTblType(),
                    userId, tblParam.getCreateTime(), tblParam.getLastAccessTime(),
                    tblParam.getLocationUrl(), tblParam.getStorageFormatId(),
                    tblParam.getFiberColId(), tblParam.getFiberFuncId());
            int resCreateTbl = dbConnection.sqlUpdate(createTblSql);
            //result
            if (resCreateTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createColumn(MetaProto.ColListType colListType,
                             StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //get a column param
            int colCount = colListType.getColumnCount();
            List<MetaProto.ColParam> columns;
            columns = colListType.getColumnList();
            MetaProto.ColParam colParam;
            colParam = columns.get(0);
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(colParam.getDbName());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, colParam.getTblName());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            //loop for every column to insert them
            for (int i = 0; i < colCount; i++) {
                colParam = columns.get(i);
                //insert column
                String createColSql = sqlGenerator.createColumn(i,
                        dbId,
                        colParam.getColName(),
                        tblId,
                        colParam.getColType(),
                        colParam.getDataType());
                int resCreateCol = dbConnection.sqlUpdate(createColSql);
                //result
                if (resCreateCol != 1) {
                    statusType = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.CREAT_COLUMN_ERROR)
                            .build();
                    responseStreamObserver.onNext(statusType);
                    responseStreamObserver.onCompleted();
                }
            }
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
    }

    @Override
    public void listDatabases(MetaProto.NoneType none,
                              StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //query
            String listDbSql = sqlGenerator.listDatabases();
            ResultList resListDb = dbConnection.sqlQuery(listDbSql, 1);
            //result
            ArrayList<String> result = new ArrayList<>();
            int size = resListDb.size();
            for (int i = 0; i < size; i++) {
                result.add(resListDb.get(i).get(0));
            }
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
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam,
                           StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbNameParam.getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StringListType stringList;
            //query
            String listTblSql = sqlGenerator.listTables(dbId);
            ResultList resListTbl = dbConnection.sqlQuery(listTblSql, 1);
            //result
            ArrayList<String> result = new ArrayList<>();
            int size = resListTbl.size();
            for (int i = 0; i < size; i++) {
                result.add(resListTbl.get(i).get(0));
            }
            stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam,
                            StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        try {
            //query
            String getDbSql = sqlGenerator.getDatabase(dbNameParam.getDatabase());
            ResultList resGetDb = dbConnection.sqlQuery(getDbSql, 3);
            //result
            //find user name
            String findUserNameSql = sqlGenerator.findUserName(Integer.parseInt(resGetDb.get(0).get(2)));
            ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
            String userName = resFindUserName.get(0).get(0);
            MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder()
                    .setDbName(resGetDb.get(0).get(0))
                    .setLocationUrl(resGetDb.get(0).get(1))
                    .setUserName(userName)
                    .build();
            responseStreamObserver.onNext(dbParam);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam,
                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            //query
            String getTblSql = sqlGenerator.getTable(dbId, dbTblParam.getTable().getTable());
            ResultList resGetTbl = dbConnection.sqlQuery(getTblSql, 8);
            //find username
            String findUserNameSql = sqlGenerator.findUserName(Integer.parseInt(resGetTbl.get(0).get(1)));
            ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
            String userName = resFindUserName.get(0).get(0);
            //result
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setDbName(dbTblParam.getDatabase().getDatabase())
                    .setTblName(dbTblParam.getTable().getTable())
                    .setTblType(Integer.parseInt(resGetTbl.get(0).get(0)))
                    .setUserName(userName)
                    .setCreateTime(Integer.parseInt(resGetTbl.get(0).get(2)))
                    .setLastAccessTime(Integer.parseInt(resGetTbl.get(0).get(3)))
                    .setLocationUrl(resGetTbl.get(0).get(4))
                    .setStorageFormatId(Integer.parseInt(resGetTbl.get(0).get(5)))
                    .setFiberColId(Integer.parseInt(resGetTbl.get(0).get(6)))
                    .setFiberFuncId(Integer.parseInt(resGetTbl.get(0).get(7)))
                    .build();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam,
                          StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblColParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, dbTblColParam.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            //query
            String getColSql = sqlGenerator.getColumn(tblId, dbTblColParam.getColumn().getColumn());
            ResultList resGetCol = dbConnection.sqlQuery(getColSql, 3);
            //result
            MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                    .setColIndex(Integer.parseInt(resGetCol.get(0).get(0)))
                    .setTblName(dbTblColParam.getTable().getTable())
                    .setColName(dbTblColParam.getColumn().getColumn())
                    .setColType(resGetCol.get(0).get(1))
                    .setDataType(resGetCol.get(0).get(2))
                    .build();
            responseStreamObserver.onNext(column);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn,
                             StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(renameColumn.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, renameColumn.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
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
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(renameTblParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
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
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
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
    }

    @Override
    public void deleteTblColumn(MetaProto.DbTblParam dbTblParam,
                                StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, dbTblParam.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            //delete column
            String deleteColSql = sqlGenerator.deleteTblColumn(dbId, tblId);
            int resDeleteCol = dbConnection.sqlUpdate(deleteColSql);
            System.out.println("delete column status is " + resDeleteCol);
            //result
            if (resDeleteCol != 0) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void deleteTable(MetaProto.DbTblParam dbTblParam,
                            StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //delete table
            String deleteTblSql = sqlGenerator.deleteTable(dbId, dbTblParam.getTable().getTable());
            int resDeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
            //result
            if (resDeleteTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void deleteDbColumn(MetaProto.DbNameParam dbNameParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbNameParam.getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //delete column
            String deleteColSql = sqlGenerator.deleteDbColumn(dbId);
            dbConnection.sqlUpdate(deleteColSql);
//            Optional<Integer> optdeleteCol = dbConnection.sqlUpdate(deleteColSql);
//            int resDeleteCol = (int) optdeleteCol.get();
//            //result
//            if (resDeleteCol != 0) {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DELETE_COLUMN_ERROR).build();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
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
    }

    @Override
    public void deleteDbTable(MetaProto.DbNameParam dbNameParam,
                              StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbNameParam.getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //delete table
            String deleteTblSql = sqlGenerator.deleteDbTable(dbId);
            dbConnection.sqlUpdate(deleteTblSql);
//            Optional<Integer> optdeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
//            int resDeleteTbl = (int) optdeleteTbl.get();
//            //result
//            if (resDeleteTbl != 0) {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
//            else {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
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
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam dbNameParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //delete database
            String deleteDbSql = sqlGenerator.deleteDatabase(dbNameParam.getDatabase());
            int resDeleteDb = dbConnection.sqlUpdate(deleteDbSql);
            //result
            MetaProto.StatusType statusType;
            if (resDeleteDb == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void createDbParam(MetaProto.DbParamParam dbParam,
                              StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbParam.getDbName());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
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
    }

    @Override
    public void createTblParam(MetaProto.TblParamParam tblParam,
                               StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(tblParam.getDbName());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, tblParam.getTblName());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
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
    }

    @Override
    public void createTblPriv(MetaProto.TblPrivParam tblPriv,
                              StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(tblPriv.getDbName());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, tblPriv.getTblName());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(tblPriv.getUserName());
            ResultList resFindUserId = dbConnection.sqlQuery(findUserIdSql, 1);
            int userId = Integer.parseInt(resFindUserId.get(0).get(0));
            //create table priv
            String createTblPrivSql = sqlGenerator.createTblPriv(
                    tblId,
                    userId,
                    tblPriv.getPrivType(),
                    tblPriv.getGrantTime());
            int resCreateTblPriv = dbConnection.sqlUpdate(createTblPrivSql);
            //result
            if (resCreateTblPriv == 1) {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
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
    }

    @Override
    public void createStorageFormat(MetaProto.StorageFormatParam storageFormat,
                                    StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
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
    }

    @Override
    public void createFiberFunc(MetaProto.FiberFuncParam fiberFunc,
                                StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
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
    }

    @Override
    public void createBlockIndex(MetaProto.BlockIndexParam blockIndex,
                                 StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(blockIndex.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            MetaProto.StatusType statusType;
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, blockIndex.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
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
    }

    @Override
    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockIndexParam,
                                 StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(
                    filterBlockIndexParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(
                    dbId,
                    filterBlockIndexParam.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndex(tblId);
                ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
                int size = resfilterBlockIndex.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndex.get(i).get(0));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexEnd(
                        tblId,
                        filterBlockIndexParam.getTimeEnd());
                ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
                int size = resfilterBlockIndex.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndex.get(i).get(0));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() != -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBegin(
                        tblId,
                        filterBlockIndexParam.getTimeBegin());
                ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
                int size = resfilterBlockIndex.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndex.get(i).get(0));
                }
            }
            else {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBeginEnd(
                        tblId,
                        filterBlockIndexParam.getTimeBegin(),
                        filterBlockIndexParam.getTimeEnd());
                ResultList resfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql, 1);
                int size = resfilterBlockIndex.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndex.get(i).get(0));
                }
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
    }

    @Override
    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam,
                                        StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(
                    filterBlockIndexByFiberParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(
                    dbId,
                    filterBlockIndexByFiberParam.getTable().getTable());
            ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
            int tblId = Integer.parseInt(resFindTblId.get(0).get(0));
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiber(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue());
                ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
                int size = resfilterBlockIndexByFiber.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndexByFiber.get(i).get(0));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeEnd());
                ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
                int size = resfilterBlockIndexByFiber.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndexByFiber.get(i).get(0));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() != -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBegin(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin());
                ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
                int size = resfilterBlockIndexByFiber.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndexByFiber.get(i).get(0));
                }
            }
            else {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBeginEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin(),
                        filterBlockIndexByFiberParam.getTimeEnd());
                ResultList resfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql, 1);
                int size = resfilterBlockIndexByFiber.size();
                for (int i = 0; i < size; i++) {
                    result.add(resfilterBlockIndexByFiber.get(i).get(0));
                }
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
    }
}
