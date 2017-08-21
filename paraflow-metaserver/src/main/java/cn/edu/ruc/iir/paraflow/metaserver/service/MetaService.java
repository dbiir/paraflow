package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.connection.SqlGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
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

    private int findDbId(String dbName)
    {
        String findDbIdSql = sqlGenerator.findDbId(dbName);
        ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
        return Integer.parseInt(resFindDbId.get(0).get(0));
    }

    private int findUserId(String userName)
    {
        String findUserIdSql = sqlGenerator.findUserId(userName);
        ResultList resFindUserId = dbConnection.sqlQuery(findUserIdSql, 1);
        return Integer.parseInt(resFindUserId.get(0).get(0));
    }

    private int findTblId(int dbId, String tblName)
    {
        String findTblIdSql = sqlGenerator.findTblId(dbId, tblName);
        ResultList resFindTblId = dbConnection.sqlQuery(findTblIdSql, 1);
        return Integer.parseInt(resFindTblId.get(0).get(0));
    }

    private String findUserName(int userId)
    {
        String findUserNameSql = sqlGenerator.findUserName(userId);
        ResultList resFindUserName = dbConnection.sqlQuery(findUserNameSql, 1);
        return resFindUserName.get(0).get(0);
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
            //find user id
            int userId = findUserId(tblParam.getUserName());
            String locationUrl = tblParam.getLocationUrl();
            if (locationUrl.equals("")) {
                metaConfig = new MetaConfig();
                String url = metaConfig.getHDFSWarehouse();
                if (url.endsWith("/")) {
                    locationUrl = String.format("%s%s", url, tblParam.getDbName());
                }
                else {
                    locationUrl = String.format("%s/%s", url, tblParam.getDbName());
                }
            }
            //create table
            String createTblSql = sqlGenerator.createTable(dbId,
                    tblParam.getTblName(), tblParam.getTblType(),
                    userId, System.currentTimeMillis(), System.currentTimeMillis(),
                    locationUrl, tblParam.getStorageFormatId(),
                    tblParam.getFiberColId(), tblParam.getFiberFuncId());
            int resCreateTbl = dbConnection.sqlUpdate(createTblSql);
            //result
            if (resCreateTbl == 1) {
                //create columns
                //get a column param
                MetaProto.ColListType colListType = tblParam.getColList();
                int colCount = colListType.getColumnCount();
                List<MetaProto.ColParam> columns;
                columns = colListType.getColumnList();
                MetaProto.ColParam colParam = columns.get(0);
                //find table id
                int tblId = findTblId(dbId, colParam.getTblName());
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
                System.out.println("sizeexecute = " + sizeexecute);
                for (int j = 0; j < sizeexecute; j++) {
                    if (colExecute[0] == 0) {
                        System.out.println("CREAT_COLUMN_ERROR");
                        statusType = MetaProto.StatusType.newBuilder()
                                .setStatus(MetaProto.StatusType.State.CREAT_COLUMN_ERROR)
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
                System.out.println("OK : " + statusType.getStatus());
                responseStreamObserver.onNext(statusType);
                responseStreamObserver.onCompleted();
                dbConnection.commit();
            }
            else {
                statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.TABLE_ALREADY_EXISTS)
                        .build();
                System.out.println("TABLE_ALREADY_EXISTS");
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
            if (!resListDb.get(0).get(0).equals("")) {
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
        dbConnection.autoCommitFalse();
    }

    @Override
    public void getTable(MetaProto.DbTblParam dbTblParam,
                         StreamObserver<MetaProto.TblParam> responseStreamObserver)
    {
        try {
            dbConnection.autoCommitTrue();
            //find database id
            String findDbIdSql = sqlGenerator.findDbId(dbTblParam.getDatabase().getDatabase());
            ResultList resFindDbId = dbConnection.sqlQuery(findDbIdSql, 1);
            int dbId = Integer.parseInt(resFindDbId.get(0).get(0));
            //query
            String getTblSql = sqlGenerator.getTable(dbId, dbTblParam.getTable().getTable());
            ResultList resGetTbl = dbConnection.sqlQuery(getTblSql, 8);
            //find username
            String userName = findUserName(Integer.parseInt(resGetTbl.get(0).get(1)));
            //result
            MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                    .setDbName(dbTblParam.getDatabase().getDatabase())
                    .setTblName(dbTblParam.getTable().getTable())
                    .setTblType(Integer.parseInt(resGetTbl.get(0).get(0)))
                    .setUserName(userName)
                    .setCreateTime(Long.parseLong(resGetTbl.get(0).get(2)))
                    .setLastAccessTime(Long.parseLong(resGetTbl.get(0).get(3)))
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
        String findParamKeySql = sqlGenerator.findParamKey(tblId);
        ResultList resFindParamKey = dbConnection.sqlQuery(findParamKeySql, 1);
        if (!resFindParamKey.isEmpty()) {
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
            }
            String deleteDbSql = sqlGenerator.deleteDatabase(dbNameParam.getDatabase());
            dbConnection.sqlUpdate(deleteDbSql);
            dbConnection.commit();
            statusType = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.OK)
                        .build();
            responseStreamObserver.onNext(statusType);
            responseStreamObserver.onCompleted();
            //result
//            if (resDeleteCol != 0) {
//                //delete table
//                String deleteTblSql = sqlGenerator.deleteDbTable(dbId);
//                int resDeleteTbl = dbConnection.sqlUpdate(deleteTblSql);
//                //result
//                if (resDeleteTbl != 0) {
//                    //delete database
//                    String deleteDbSql = sqlGenerator.deleteDatabase(dbNameParam.getDatabase());
//                    int resDeleteDb = dbConnection.sqlUpdate(deleteDbSql);
//                    //result
//                    if (resDeleteDb == 1) {
//                        statusType = MetaProto.StatusType.newBuilder()
//                                .setStatus(MetaProto.StatusType.State.OK)
//                                .build();
//                        dbConnection.commit();
//                        responseStreamObserver.onNext(statusType);
//                        responseStreamObserver.onCompleted();
//                    }
//                    else {
//                        statusType = MetaProto.StatusType.newBuilder()
//                                .setStatus(MetaProto.StatusType.State.DELETE_DATABASE_ERROR)
//                                .build();
//                        dbConnection.rollback();
//                        responseStreamObserver.onNext(statusType);
//                        responseStreamObserver.onCompleted();
//                    }
//                }
//                else {
//                    statusType = MetaProto.StatusType.newBuilder()
//                            .setStatus(MetaProto.StatusType.State.DELETE_TABLE_ERROR)
//                            .build();
//                    dbConnection.rollback();
//                    responseStreamObserver.onNext(statusType);
//                    responseStreamObserver.onCompleted();
//                }
//            }
//            else {
//                statusType = MetaProto.StatusType.newBuilder()
//                        .setStatus(MetaProto.StatusType.State.DELETE_COLUMN_ERROR)
//                        .build();
//                dbConnection.rollback();
//                responseStreamObserver.onNext(statusType);
//                responseStreamObserver.onCompleted();
//            }
        }
        catch (NullPointerException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
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
        dbConnection.autoCommitFalse();
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
