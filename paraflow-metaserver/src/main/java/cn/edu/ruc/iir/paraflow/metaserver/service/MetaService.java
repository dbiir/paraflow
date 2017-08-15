package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.connection.SqlGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

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
            //get a columnparam
            int colCount = colListType.getColumnCount();
            List<MetaProto.ColParam> columns = new ArrayList<>();
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
            int tblId = Integer.parseInt(resFindDbId.get(0).get(0));
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
                if (resCreateCol == 1) {
                    statusType = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.OK)
                            .build();
                    continue;
                }
                else {
                    statusType = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS)
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
            Optional optListDb = dbConnection.sqlQuery(listDbSql);
            ResultSet resListDb = (ResultSet) optListDb.get();
            //result
            ArrayList<String> result = new ArrayList<>();
            while (resListDb.next()) {
                result.add(resListDb.getString(1));
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StringListType stringList;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                System.out.println("database not found");
            }
            //query
            String listTblSql = sqlGenerator.listTables(dbId);
            Optional<ResultSet> optListTbl = dbConnection.sqlQuery(listTblSql);
            ResultSet resListTbl = (ResultSet) optListTbl.get();
            //result
            ArrayList<String> result = new ArrayList<>();
            while (resListTbl.next()) {
                result.add(resListTbl.getString(1));
            }
            stringList = MetaProto.StringListType.newBuilder().addAllStr(result).build();
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
    public void getDatabase(MetaProto.DbNameParam dbNameParam,
                            StreamObserver<MetaProto.DbParam> responseStreamObserver)
    {
        try {
            //query
            String getDbSql = sqlGenerator.getDatabase(dbNameParam.getDatabase());
            Optional<ResultSet> optGetDb = dbConnection.sqlQuery(getDbSql);
            ResultSet resGetDb = (ResultSet) optGetDb.get();
            //result
            if (resGetDb.next()) {
                //find user name
                String findUserNameSql = sqlGenerator.findUserName(resGetDb.getInt(3));
                Optional<ResultSet> optFindUserName = dbConnection.sqlQuery(findUserNameSql);
                ResultSet resFindUserName = (ResultSet) optFindUserName.get();
                String userName = "";
                MetaProto.StringListType stringList;
                if (resFindUserName.next()) {
                    userName = resFindUserName.getString(1);
                    MetaProto.DbParam dbParam = MetaProto.DbParam.newBuilder()
                            .setDbName(resGetDb.getString(1))
                            .setLocationUrl(resGetDb.getString(2))
                            .setUserName(userName)
                            .build();
                    responseStreamObserver.onNext(dbParam);
                    responseStreamObserver.onCompleted();
                }
                else {
                    System.out.println("user not found");
                }
            }
            else {
                System.out.println("database not found");
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
    public void getTable(MetaProto.DbTblParam dbTblParam,
                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                System.out.println("database not found");
            }
            //query
            String getTblSql = sqlGenerator.getTable(dbId, dbTblParam.getTable().getTable());
            Optional<ResultSet> optGetTbl = dbConnection.sqlQuery(getTblSql);
            ResultSet resGetTbl = (ResultSet) optGetTbl.get();
            String userName = "";
            if (resGetTbl.next()) {
                //find username
                String findUserNameSql = sqlGenerator.findUserName(resGetTbl.getInt(2));
                Optional<ResultSet> optFindUserName = dbConnection.sqlQuery(findUserNameSql);
                ResultSet resFindUserName = (ResultSet) optFindUserName.get();
                if (resFindUserName.next()) {
                    userName = resFindUserName.getString(1);
                }
                else {
                    System.out.println("database not found");
                }
                //result
                MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                        .setDbName(dbTblParam.getDatabase().getDatabase())
                        .setTblName(dbTblParam.getTable().getTable())
                        .setTblType(resGetTbl.getInt(1))
                        .setUserName(userName)
                        .setCreateTime(resGetTbl.getInt(3))
                        .setLastAccessTime(resGetTbl.getInt(4))
                        .setLocationUrl(resGetTbl.getString(5))
                        .setStorageFormatId(resGetTbl.getInt(6))
                        .setFiberColId(resGetTbl.getInt(7))
                        .setFiberFuncId(resGetTbl.getInt(8))
                        .build();
                responseStreamObserver.onNext(tblParam);
                responseStreamObserver.onCompleted();
            }
            else {
                System.out.println("table not found");
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
    public void getColumn(MetaProto.DbTblColParam dbTblColParam,
                          StreamObserver<MetaProto.ColParam> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblColParam.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                System.out.println("database not found");
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, dbTblColParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                System.out.println("table not found");
            }
            //query
            String getColSql = sqlGenerator.getColumn(tblId, dbTblColParam.getColumn().getColumn());
            Optional<ResultSet> optGetCol = dbConnection.sqlQuery(getColSql);
            ResultSet resGetCol = (ResultSet) optGetCol.get();
            if (resGetCol.next()) {
                //result
                MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                        .setColIndex(resGetCol.getInt(1))
                        .setTblName(dbTblColParam.getTable().getTable())
                        .setColName(resGetCol.getString(3))
                        .setColType(resGetCol.getString(4))
                        .setDataType(resGetCol.getString(5))
                        .build();
                responseStreamObserver.onNext(column);
                responseStreamObserver.onCompleted();
            }
            else {
                System.out.println("column not found");
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
    public void renameColumn(MetaProto.RenameColParam renameColumn,
                             StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
        try {
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(renameColumn.getDatabase().getDatabase());
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, renameColumn.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, dbTblParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, tblParam.getTblName());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, tblPriv.getTblName());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find user id
            String findUserIdSql = sqlGenerator.findUserId(tblPriv.getUserName());
            Optional<ResultSet> optFindUserId = dbConnection.sqlQuery(findUserIdSql);
            ResultSet resFindUserId = (ResultSet) optFindUserId.get();
            int userId = 0;
            if (resFindUserId.next()) {
                userId = resFindUserId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.USER_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            System.out.println("userid = " + userId);
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            MetaProto.StatusType statusType;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(dbId, blockIndex.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_NOT_FOUND)
                        .build();
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
            }
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                System.out.println("database not found");
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(
                    dbId,
                    filterBlockIndexParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                System.out.println("table not found");
            }
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndex(tblId);
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() == -1
                    && filterBlockIndexParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexEnd(
                        tblId,
                        filterBlockIndexParam.getTimeEnd());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else if (filterBlockIndexParam.getTimeBegin() != -1
                    && filterBlockIndexParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBegin(
                        tblId,
                        filterBlockIndexParam.getTimeBegin());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
                }
            }
            else {
                //query
                String filterBlockIndexSql = sqlGenerator.filterBlockIndexBeginEnd(
                        tblId,
                        filterBlockIndexParam.getTimeBegin(),
                        filterBlockIndexParam.getTimeEnd());
                Optional optfilterBlockIndex = dbConnection.sqlQuery(filterBlockIndexSql);
                ResultSet resfilterBlockIndex = (ResultSet) optfilterBlockIndex.get();
                while (resfilterBlockIndex.next()) {
                    result.add(resfilterBlockIndex.getString(1));
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
        catch (java.sql.SQLException e) {
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
            Optional<ResultSet> optFindDbId = dbConnection.sqlQuery(findDbIdSql);
            ResultSet resFindDbId = (ResultSet) optFindDbId.get();
            int dbId = 0;
            if (resFindDbId.next()) {
                dbId = resFindDbId.getInt(1);
            }
            else {
                System.out.println("database not found");
            }
            //find table id
            String findTblIdSql = sqlGenerator.findTblId(
                    dbId,
                    filterBlockIndexByFiberParam.getTable().getTable());
            Optional<ResultSet> optFindTblId = dbConnection.sqlQuery(findTblIdSql);
            ResultSet resFindTblId = (ResultSet) optFindTblId.get();
            int tblId = 0;
            if (resFindTblId.next()) {
                tblId = resFindTblId.getInt(1);
            }
            else {
                System.out.println("table not found");
            }
            ArrayList<String> result = new ArrayList<>();
            if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiber(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() != -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeEnd());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() != -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBegin(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
                }
            }
            else {
                //query
                String filterBlockIndexByFiberSql = sqlGenerator.filterBlockIndexByFiberBeginEnd(
                        tblId,
                        filterBlockIndexByFiberParam.getValue().getValue(),
                        filterBlockIndexByFiberParam.getTimeBegin(),
                        filterBlockIndexByFiberParam.getTimeEnd());
                Optional optfilterBlockIndexByFiber = dbConnection.sqlQuery(filterBlockIndexByFiberSql);
                ResultSet resfilterBlockIndexByFiber = (ResultSet) optfilterBlockIndexByFiber.get();
                while (resfilterBlockIndexByFiber.next()) {
                    result.add(resfilterBlockIndexByFiber.getString(1));
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
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
