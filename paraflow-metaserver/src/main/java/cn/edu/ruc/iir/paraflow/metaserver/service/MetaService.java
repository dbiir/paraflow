package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import cn.edu.ruc.iir.paraflow.metaserver.utils.DBConnection;
import io.grpc.stub.StreamObserver;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase
{
    SqlGenerator sqlGenerator = new SqlGenerator();
    DBConnection dbConnection = DBConnection.getConnectionInstance();
    @Override
    public void createUser(MetaProto.CreateUserParam createUser, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //create user
            String createUserSql = sqlGenerator.createUser(createUser.getUserName(), createUser.getCreateTime(), createUser.getLastVisitTime());
            Optional<Integer> optCreateUser = dbConnection.sqlUpdate(createUserSql);
            int resCreateUser = (int) optCreateUser.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateUser == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        MetaProto.StatusType statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        responseStreamObserver.onNext(statusType);
        responseStreamObserver.onCompleted();
    }

    @Override
    public void createDatabase(MetaProto.DbParam dbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(dbParam.getUserName());
            Optional<ResultSet> optFindUserId = dbConnection.sqlQuery(findUserIdSql);
            ResultSet resFindUserId = (ResultSet) optFindUserId.get();
            int userId = resFindUserId.getInt(1);
            //create database
            String createDbSql = sqlGenerator.createDatabase(dbParam.getDbName(), userId, dbParam.getLocationUrl());
            Optional<Integer> optCreateDB = dbConnection.sqlUpdate(createDbSql);
            int resCreateDb = (int) optCreateDB.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateDb == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createTable(MetaProto.TblParam tblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(tblParam.getDbName());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(tblParam.getUserName());
            Optional<ResultSet> optFindUserId = dbConnection.sqlQuery(findUserIdSql);
            ResultSet resFindUserId = (ResultSet) optFindUserId.get();
            int userId = resFindUserId.getInt(1);
            //create table
            String createTblSql = sqlGenerator.createTable(dbId,
                    tblParam.getTblName(), tblParam.getTblType(),
                    userId, tblParam.getCreateTime(), tblParam.getLastAccessTime(),
                    tblParam.getLocationUrl(), tblParam.getStorageFormatId(),
                    tblParam.getFiberColId(), tblParam.getFiberFuncId());
            Optional<Integer> optCreateTbl = dbConnection.sqlUpdate(createTblSql);
            int resCreateTbl = (int) optCreateTbl.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createColumn(MetaProto.ColListType colListType, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //get a columnparam
            int colCount = colListType.getColumnCount();
            List<MetaProto.ColParam> columns = new ArrayList<>();
            columns = colListType.getColumnList();
            MetaProto.ColParam colParam;
            colParam = columns.get(0);
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(colParam.getDbName());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, colParam.getTblName());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //loop for every column to insert them
            MetaProto.StatusType statusType;
            for (int i = 0; i < colCount; i++) {
                colParam = columns.get(i);
                //insert column
                String createColSql = sqlGenerator.createColumn(i, colParam.getColName(), tblId, colParam.getColType(), colParam.getDataType());
                Optional<Integer> optCreateCol = dbConnection.sqlUpdate(createColSql);
                int resCreateCol = (int) optCreateCol.get();
                //result
                if (resCreateCol == 1) {
                    statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                    continue;
                }
                else {
                    statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS).build();
                    responseStreamObserver.onNext(statusType);
                    responseStreamObserver.onCompleted();
                }
            }
            statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void listDatabases(MetaProto.NoneType none, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //query
            String listDbSql = sqlGenerator.listDatabases();
            Optional optListDb = dbConnection.sqlQuery(listDbSql);
            ResultSet resListDb = (ResultSet) optListDb.get();
            //result
            ArrayList<String> result = new ArrayList<>();
            while (resListDb.next()) {
                result.add(resListDb.getString(1));
            }
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void listTables(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbNameParam.getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //query
            String listTblSql = sqlGenerator.listTables(dbId);
            Optional<ResultSet> optListTbl = dbConnection.sqlQuery(listTblSql);
            ResultSet resListTbl = (ResultSet) optListTbl.get();
            //result
            ArrayList<String> result = new ArrayList<>();
            while (resListTbl.next()) {
                result.add(resListTbl.getString(1));
            }
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        try {
            //query
            String getDbSql = sqlGenerator.getDatabase(dbNameParam.getDatabase());
            Optional<ResultSet> optGetDb = dbConnection.sqlQuery(getDbSql);
            ResultSet resGetDb = (ResultSet) optGetDb.get();
            //result
            MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder().setDbName(resGetDb.getString(1)).setLocationUrl(resGetDb.getString(2)).setUserName(resGetDb.getString(3)).build();
            responseStreamObserver.onNext(dbParam);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //query
            String getTblSql = sqlGenerator.getTable(dbId, dbTblParam.getTable().getTable());
            Optional<ResultSet> optGetTbl = dbConnection.sqlQuery(getTblSql);
            ResultSet resGetTbl = (ResultSet) optGetTbl.get();
            //find username
            String findUserNameSql = sqlGenerator.findUserName(resGetTbl.getInt(2));
            Optional<ResultSet> optFindUserName = dbConnection.sqlQuery(findUserNameSql);
            ResultSet resFindUserName = (ResultSet) optFindUserName.get();
            String userName = resFindUserName.getString(1);
            //result
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setDbName(dbTblParam.getDatabase().getDatabase())
                    .setTblName(dbTblParam.getTable().getTable())
                    .setTblType(resGetTbl.getInt(1)).setUserName(userName)
                    .setCreateTime(resGetTbl.getInt(3)).setLastAccessTime(resGetTbl.getInt(4))
                    .setLocationUrl(resGetTbl.getString(5)).setStorageFormatId(resGetTbl.getInt(6))
                    .setFiberColId(resGetTbl.getInt(7)).setFiberFuncId(resGetTbl.getInt(8)).build();
            responseStreamObserver.onNext(tblParam);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void getColumn(MetaProto.DbTblColParam dbTblColParam, StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblColParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, dbTblColParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //query
            String getColSql = sqlGenerator.getColumn(tblId, dbTblColParam.getColumn().getColumn());
            Optional<ResultSet> optGetCol = dbConnection.sqlQuery(getColSql);
            ResultSet resGetCol = (ResultSet) optGetCol.get();
            //result
            MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                    .setColIndex(resGetCol.getInt(1)).setTblName(dbTblColParam.getTable().getTable())
                    .setColName(resGetCol.getString(3)).setColType(resGetCol.getString(4)).
                            setDataType(resGetCol.getString(5)).build();
            responseStreamObserver.onNext(column);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void renameColumn(MetaProto.RenameColParam renameColumn, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(renameColumn.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, renameColumn.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //rename column
            String renameColSql = sqlGenerator.renameColumn(dbId, tblId, renameColumn.getOldName(), renameColumn.getNewName());
            Optional<Integer> optRenameCol = dbConnection.sqlUpdate(renameColSql);
            int resRenameCol = (int) optRenameCol.get();
            //result
            MetaProto.StatusType statusType;
            if (resRenameCol == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void renameTable(MetaProto.RenameTblParam renameTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(renameTblParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //rename table
            String renameTblSql = sqlGenerator.renameTable(dbId, renameTblParam.getOldName(), renameTblParam.getNewName());
            Optional<Integer> optRenameTbl = dbConnection.sqlUpdate(renameTblSql);
            int resRenameTbl = (int) optRenameTbl.get();
            //result
            MetaProto.StatusType statusType;
            if (resRenameTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void renameDatabase(MetaProto.RenameDbParam renameDbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //rename database
            String renameDbSql = sqlGenerator.renameDatabase(renameDbParam.getOldName(), renameDbParam.getNewName());
            Optional<Integer> optRenameDb = dbConnection.sqlUpdate(renameDbSql);
            int resRenameDb = (int) optRenameDb.get();
            //result
            MetaProto.StatusType statusType;
            if (resRenameDb == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
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
    public void deleteTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //delete table
            String deleteTblSql = sqlGenerator.deleteTable(dbId, dbTblParam.getTable().getTable());
            Optional<Integer> optdeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
            int resDeleteTbl = (int) optdeleteTbl.get();
            //result
            MetaProto.StatusType statusType;
            if (resDeleteTbl == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void deleteDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //delete database
            String deleteDbSql = sqlGenerator.deleteDatabase(dbNameParam.getDatabase());
            Optional<Integer> optDeleteDb = dbConnection.sqlUpdate(deleteDbSql);
            int resDeleteDb = (int) optDeleteDb.get();
            //result
            MetaProto.StatusType statusType;
            if (resDeleteDb == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
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
    public void createDbParam(MetaProto.CreateDbParamParam createDbParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(createDbParam.getDbName());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //create database param
            String createDbParamSql = sqlGenerator.createDbParam(dbId, createDbParam.getParamKey(), createDbParam.getParamValue());
            Optional<Integer> optCreateDbParam = dbConnection.sqlUpdate(createDbParamSql);
            int resCreateDbParam = (int) optCreateDbParam.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateDbParam == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createTblParam(MetaProto.CreateTblParamParam createTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(createTblParam.getDbName());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, createTblParam.getTblName());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //create table param
            String createTblParamSql = sqlGenerator.createTblParam(tblId, createTblParam.getParamKey(), createTblParam.getParamValue());
            Optional<Integer> optCreateTblParam = dbConnection.sqlUpdate(createTblParamSql);
            int resCreateTblParam = (int) optCreateTblParam.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateTblParam == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createTblPriv(MetaProto.CreateTblPrivParam createTblPriv, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(createTblPriv.getDbName());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, createTblPriv.getTblName());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(createTblPriv.getUserName());
            Optional<ResultSet> optFindUserId = dbConnection.sqlQuery(findUserIdSql);
            ResultSet resFindUserId = (ResultSet) optFindUserId.get();
            int userId = resFindUserId.getInt(1);
            //create table priv
            String createTblPrivSql = sqlGenerator.createTblPriv(tblId, userId, createTblPriv.getPrivType(), createTblPriv.getGrantTime());
            Optional<Integer> optCreateTblPriv = dbConnection.sqlUpdate(createTblPrivSql);
            int resCreateTblPriv = (int) optCreateTblPriv.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateTblPriv == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void createStorageFormat(MetaProto.CreateStorageFormatParam createStorageFormat, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //create storage format
            String createStorageFormatSql = sqlGenerator.createStorageFormat(createStorageFormat.getStorageFormatName(), createStorageFormat.getCompression(), createStorageFormat.getSerialFormat());
            Optional<Integer> optCreateStorageFormat = dbConnection.sqlUpdate(createStorageFormatSql);
            int resCreateStorageFormat = (int) optCreateStorageFormat.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateStorageFormat == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
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
    public void createFiberFunc(MetaProto.CreateFiberFuncParam createFiberFunc, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //create storage format
            String createFiberFuncSql = sqlGenerator.createFiberFunc(createFiberFunc.getFiberFuncName(), createFiberFunc.getFiberFuncContent());
            Optional<Integer> optCreateFiberFunc = dbConnection.sqlUpdate(createFiberFuncSql);
            int resCreateFiberFunc = (int) optCreateFiberFunc.get();
            //result
            MetaProto.StatusType statusType;
            if (resCreateFiberFunc == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
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
    public void createBlockIndex(MetaProto.CreateBlockIndexParam createBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(createBlockIndex.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, createBlockIndex.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            //create storage format
            String createBlockIndexSql = sqlGenerator.createBlockIndex(tblId, createBlockIndex.getValue().getValue(), createBlockIndex.getTimeBegin(), createBlockIndex.getTimeEnd(), createBlockIndex.getTimeZone(), createBlockIndex.getBlockPath());
            Optional<Integer> optcreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
            int rescreateBlockIndex = (int) optcreateBlockIndex.get();
            //result
            MetaProto.StatusType statusType;
            if (rescreateBlockIndex == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.DATABASE_ALREADY_EXISTS).build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void filterBlockIndex(MetaProto.FilterBlockIndexParam filterBlockIndexParam, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(filterBlockIndexParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, filterBlockIndexParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexParam.getTimeBegin() == -1 && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndex(tblId);
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() == -1 && filterBlockIndexParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexEnd(tblId, filterBlockIndexParam.getTimeEnd());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() != -1 && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBegin(tblId, filterBlockIndexParam.getTimeBegin());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBeginEnd(tblId, filterBlockIndexParam.getTimeBegin(), filterBlockIndexParam.getTimeEnd());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            //result
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void filterBlockIndexByFiber(MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(filterBlockIndexByFiberParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = resFindDbId.getInt(1);
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, filterBlockIndexByFiberParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = resFindTblId.getInt(1);
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexByFiberParam.getTimeBegin() == -1 && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiber(tblId, filterBlockIndexByFiberParam.getValue().getValue());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1 && filterBlockIndexByFiberParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberEnd(tblId, filterBlockIndexByFiberParam.getValue().getValue(), filterBlockIndexByFiberParam.getTimeEnd());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() != -1 && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBegin(tblId, filterBlockIndexByFiberParam.getValue().getValue(), filterBlockIndexByFiberParam.getTimeBegin());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBeginEnd(tblId, filterBlockIndexByFiberParam.getValue().getValue(), filterBlockIndexByFiberParam.getTimeBegin(), filterBlockIndexByFiberParam.getTimeEnd());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            //result
            MetaProto.StringListType stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
            responseStreamObserver.onNext(stringList);
            responseStreamObserver.onCompleted();
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
