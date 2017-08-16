package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ParaFlow
 *
 * @author guodong
 */

// TODO add security mechanism for rpc communication
public class MetaClient
{
    private static final Logger logger = Logger.getLogger(MetaClient.class.getName());

    private final ManagedChannel channel;
    private final MetaGrpc.MetaBlockingStub metaBlockingStub;
    private DBConnection dbConnection = DBConnection.getConnectionInstance();

    public MetaClient(String host, int port)
    {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    private MetaClient(ManagedChannel channel)
    {
        this.channel = channel;
        this.metaBlockingStub = MetaGrpc.newBlockingStub(channel);
        logger.info("***Client started.");
    }

    public void commitRollBack(MetaProto.StatusType statusType)
    {
        if (statusType.getStatus() == MetaProto.StatusType.State.OK) {
            dbConnection.commit();
        }
        else {
            dbConnection.rollback();
        }
    }

    public void shutdown(int pollSecs) throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(pollSecs, TimeUnit.SECONDS);
    }

    public MetaProto.StatusType createUser(String userName, String password)
    {
        long createTime = System.currentTimeMillis();
        long lastVisitTime = System.currentTimeMillis();
        MetaProto.UserParam user =
                MetaProto.UserParam.newBuilder()
                        .setUserName(userName)
                        .setCreateTime(createTime)
                        .setLastVisitTime(lastVisitTime)
                        .setPassword(password)
                        .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createUser(user);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create user status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createDatabase(String dbName, String userName)
    {
        // get config instance
        MetaConfig metaConfig = null;
        try {
            metaConfig = new MetaConfig("/home/jelly/Developer/paraflow/dist/paraflow-1.0-alpha1/conf/metaserver.conf");
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        // locationurl
        String url = metaConfig.getHDFSWarehouse();
        String locationUrl;
        if (url.endsWith("/")) {
            locationUrl = String.format("%s%s", url, dbName);
        }
        else {
            locationUrl = String.format("%s/%s", url, dbName);
        }
        return createDatabase(dbName, locationUrl, userName);
    }

    public MetaProto.StatusType createDatabase(String dbName, String locationUrl, String userName)
    {
        MetaProto.DbParam database = MetaProto.DbParam.newBuilder()
                .setDbName(dbName)
                .setLocationUrl(locationUrl)
                .setUserName(userName)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createDatabase(database);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create database status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTable(
            String dbName,
            String tblName,
            int tblType,
            String userName,
            int storageFormatId,
            int fiberColId,
            int fiberFuncId,
            ArrayList<String> columnName,
            ArrayList<String> columnType,
            ArrayList<String> dataType)
    {
        // get config instance
        MetaConfig metaConfig = null;
        try {
            metaConfig = new MetaConfig("/home/jelly/Developer/paraflow/dist/paraflow-1.0-alpha1/conf/metaserver.conf");
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        // locationurl
        String url = metaConfig.getHDFSWarehouse();
        String locationUrl;
        if (url.endsWith("/")) {
            locationUrl = String.format("%s%s/%s", url, dbName, tblName);
        }
        else {
            locationUrl = String.format("%s/%s/%s", url, dbName, tblName);
        }
        return createTable(
                dbName,
                tblName,
                tblType,
                userName,
                locationUrl,
                storageFormatId,
                fiberColId,
                fiberFuncId,
                columnName,
                columnType,
                dataType);
    }

    public MetaProto.StatusType createTable(
            String dbName,
            String tblName,
            int tblType,
            String userName,
            String locationUrl,
            int storageFormatId,
            int fiberColId,
            int fiberFuncId,
            ArrayList<String> columnName,
            ArrayList<String> columnType,
            ArrayList<String> dataType)
    {
        long createTime = System.currentTimeMillis();
        long lastAccessTime = System.currentTimeMillis();
        int number = columnName.size();
        ArrayList<MetaProto.ColParam> columns = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                    .setColIndex(i)
                    .setDbName(dbName)
                    .setTblName(tblName)
                    .setColName(columnName.get(i))
                    .setColType(columnType.get(i))
                    .setDataType(dataType.get(i))
                    .build();
            columns.add(column);
        }
        MetaProto.ColListType colList = MetaProto.ColListType.newBuilder()
                .addAllColumn(columns)
                .build();
        MetaProto.TblParam table = MetaProto.TblParam.newBuilder()
                .setDbName(dbName)
                .setTblName(tblName)
                .setUserName(userName)
                .setTblType(tblType)
                .setCreateTime(createTime)
                .setLastAccessTime(lastAccessTime)
                .setLocationUrl(locationUrl)
                .setStorageFormatId(storageFormatId)
                .setFiberColId(fiberColId)
                .setFiberFuncId(fiberFuncId)
                .build();
        MetaProto.StatusType statusColumn;
        MetaProto.StatusType statusTable;
        try {
            statusTable = metaBlockingStub.createTable(table);
            if (statusTable.getStatus() == MetaProto.StatusType.State.OK) {
                statusColumn = metaBlockingStub.createColumn(colList);
                if (statusColumn.getStatus() == MetaProto.StatusType.State.OK) {
                    dbConnection.commit();
                }
                else {
                    dbConnection.rollback();
                }
            }
            else {
                MetaProto.StatusType statusError = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.CREATE_TABLE_ERROR)
                        .build();
                dbConnection.rollback();
                return statusError;
            }
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            MetaProto.StatusType statusError = MetaProto.StatusType.newBuilder()
                    .setStatus(MetaProto.StatusType.State.CREAT_COLUMN_ERROR)
                    .build();
            return statusError;
        }
        logger.info("Create table status is : " + statusColumn.getStatus());
        return statusColumn;
    }

    public MetaProto.StringListType listDatabases()
    {
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listDatabases(none);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Database list : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType listTables(String dbName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listTables(databaseName);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Table list : " + stringList);
        return stringList;
    }

    public MetaProto.DbParam getDatabase(String dbName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.DbParam database;
        try {
            database = metaBlockingStub.getDatabase(databaseName);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            database = MetaProto.DbParam.newBuilder().build();
            return database;
        }
        logger.info("Database is : " + database);
        return  database;
    }

    public MetaProto.TblParam getTable(String dbName, String tblName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.DbTblParam databaseTable = MetaProto.DbTblParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .build();
        MetaProto.TblParam table;
        try {
            table = metaBlockingStub.getTable(databaseTable);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            table = MetaProto.TblParam.newBuilder().build();
            return table;
        }
        logger.info("Table is : " + table);
        return table;
    }

    public MetaProto.ColParam getColumn(String dbName, String tblName, String colName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.ColNameParam columnName = MetaProto.ColNameParam.newBuilder()
                .setColumn(colName)
                .build();
        MetaProto.DbTblColParam databaseTableColumn = MetaProto.DbTblColParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .setColumn(columnName)
                .build();
        MetaProto.ColParam column;
        try {
            column = metaBlockingStub.getColumn(databaseTableColumn);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            column = MetaProto.ColParam.newBuilder().build();
            return column;
        }
        logger.info("Column is : " + column);
        return column;
    }

    public MetaProto.StatusType renameColumn(String dbName,
                                             String tblName,
                                             String oleName,
                                             String newName)
    {
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.RenameColParam renameColumn = MetaProto.RenameColParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .setOldName(oleName)
                .setNewName(newName)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameColumn(renameColumn);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType renameTable(String dbName, String oldName, String newName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.RenameTblParam renameTable = MetaProto.RenameTblParam.newBuilder()
                .setDatabase(databaseName)
                .setOldName(oldName)
                .setNewName(newName)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameTable(renameTable);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename table status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType renameDatabase(String oldName, String newName)
    {
        MetaProto.RenameDbParam renameDatabase = MetaProto.RenameDbParam.newBuilder()
                .setOldName(oldName)
                .setNewName(newName)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameDatabase(renameDatabase);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename database status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType deleteTable(String dbName, String tblName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.DbTblParam databaseTable = MetaProto.DbTblParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .build();
        MetaProto.StatusType statusColumn;
        MetaProto.StatusType statusTable;
        try {
            statusColumn = metaBlockingStub.deleteTblColumn(databaseTable);
            System.out.println("delete column state is : " + statusColumn.getStatus());
            if (statusColumn.getStatus() == MetaProto.StatusType.State.OK) {
                statusTable = metaBlockingStub.deleteTable(databaseTable);
                if (statusTable.getStatus() == MetaProto.StatusType.State.OK) {
                    dbConnection.commit();
                }
                else {
                    dbConnection.rollback();
                }
            }
            else {
                MetaProto.StatusType statusError = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DELETE_COLUMN_ERROR)
                        .build();
                dbConnection.rollback();
                return statusError;
            }
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            statusTable = MetaProto.StatusType.newBuilder().build();
            return statusTable;
        }
        logger.info("Delete table status is : " + statusTable.getStatus());
        return statusTable;
    }

    public MetaProto.StatusType deleteDatabase(String dbName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.StatusType statusDb;
        MetaProto.StatusType statusCol;
        MetaProto.StatusType statusTbl;
        try {
            statusCol = metaBlockingStub.deleteDbColumn(databaseName);
            if (statusCol.getStatus() == MetaProto.StatusType.State.OK) {
                statusTbl = metaBlockingStub.deleteDbTable(databaseName);
                if (statusTbl.getStatus() == MetaProto.StatusType.State.OK) {
                    statusDb = metaBlockingStub.deleteDatabase(databaseName);
                    if (statusDb.getStatus() == MetaProto.StatusType.State.OK) {
                        dbConnection.commit();
                    }
                    else {
                        dbConnection.rollback();
                    }
            }
            else {
                    MetaProto.StatusType statusError = MetaProto.StatusType.newBuilder()
                            .setStatus(MetaProto.StatusType.State.DELETE_TABLE_ERROR)
                            .build();
                    dbConnection.rollback();
                    return statusError;
                }
            }
            else {
                MetaProto.StatusType statusError = MetaProto.StatusType.newBuilder()
                        .setStatus(MetaProto.StatusType.State.DELETE_COLUMN_ERROR)
                        .build();
                dbConnection.rollback();
                return statusError;
            }
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            statusDb = MetaProto.StatusType.newBuilder().build();
            return statusDb;
        }
        logger.info("Delete database status is : " + statusDb.getStatus());
        return statusDb;
    }

    public MetaProto.StatusType createDbParam(String dbName, String paramKey, String paramValue)
    {
        MetaProto.DbParamParam dbParam = MetaProto.DbParamParam.newBuilder()
                .setDbName(dbName)
                .setParamKey(paramKey)
                .setParamValue(paramValue)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createDbParam(dbParam);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create database param status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTblParam(String dbName,
                                               String tblName,
                                               String paramKey,
                                               String paramValue)
    {
        MetaProto.TblParamParam tblParam = MetaProto.TblParamParam.newBuilder()
                .setDbName(dbName)
                .setTblName(tblName)
                .setParamKey(paramKey)
                .setParamValue(paramValue)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTblParam(tblParam);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create table param status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTblPriv(String dbName,
                                              String tblName,
                                              String userName,
                                              int privType)
    {
        long grantTime = System.currentTimeMillis();
        MetaProto.TblPrivParam tblPriv = MetaProto.TblPrivParam.newBuilder()
                .setDbName(dbName)
                .setTblName(tblName)
                .setUserName(userName)
                .setPrivType(privType)
                .setGrantTime(grantTime)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTblPriv(tblPriv);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create tblpriv status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createStorageFormat(String storageFormatName,
                                                    String compression,
                                                    String serialFormat)
    {
        MetaProto.StorageFormatParam storageFormat
                = MetaProto.StorageFormatParam.newBuilder()
                .setStorageFormatName(storageFormatName)
                .setCompression(compression)
                .setSerialFormat(serialFormat)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createStorageFormat(storageFormat);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create storage format status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createFiberFunc(String fiberFuncName, ByteString fiberFuncContent)
    {
        MetaProto.FiberFuncParam fiberFunc = MetaProto.FiberFuncParam.newBuilder()
                .setFiberFuncName(fiberFuncName)
                .setFiberFuncContent(fiberFuncContent).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createFiberFunc(fiberFunc);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create fiber function status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createBlockIndex(String dbName,
                                                 String tblName,
                                                 int value,
                                                 long timeBegin,
                                                 long timeEnd,
                                                 String path)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder()
                .setValue(value)
                .build();
        MetaProto.BlockIndexParam blockIndex = MetaProto.BlockIndexParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .setValue(fiberValue)
                .setTimeBegin(timeBegin)
                .setTimeEnd(timeEnd)
                .setBlockPath(path)
                .build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createBlockIndex(blockIndex);
            commitRollBack(status);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create block index status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StringListType filterBlockIndex(String dbName,
                                                     String tblName,
                                                     long timeBegin,
                                                     long timeEnd)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.FilterBlockIndexParam filterBlockIndex = MetaProto.FilterBlockIndexParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .setTimeBegin(timeBegin)
                .setTimeEnd(timeEnd)
                .build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.filterBlockIndex(filterBlockIndex);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Filter block paths by time is : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType filterBlockIndexByFiber(String dbName,
                                                            String tblName,
                                                            int value,
                                                            long timeBegin,
                                                            long timeEnd)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder()
                .setValue(value)
                .build();
        MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiber
                = MetaProto.FilterBlockIndexByFiberParam.newBuilder()
                .setDatabase(databaseName)
                .setTable(tableName)
                .setValue(fiberValue)
                .setTimeBegin(timeBegin)
                .setTimeEnd(timeEnd)
                .build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.filterBlockIndexByFiber(filterBlockIndexByFiber);
            dbConnection.commit();
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Filter block paths is : " + stringList);
        return stringList;
    }
}
