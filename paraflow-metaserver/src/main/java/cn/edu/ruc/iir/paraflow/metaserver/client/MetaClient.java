package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.ColType;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO add security mechanism for rpc communication
// todo add name pattern constraint
public class MetaClient
{
    private static final Logger logger = Logger.getLogger(MetaClient.class.getName());

    private final ManagedChannel channel;
    private final MetaGrpc.MetaBlockingStub metaBlockingStub;

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

    public void shutdown(int pollSecs) throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(pollSecs, TimeUnit.SECONDS);
    }

    public StatusProto.ResponseStatus createUser(String userName, String password)
    {
        MetaProto.UserParam user =
                MetaProto.UserParam.newBuilder()
                        .setUserName(userName)
                        .setPassword(password)
                        .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createUser(user);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create user status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createDatabase(String dbName, String userName)
    {
        String locationUrl = "";
        return createDatabase(dbName, locationUrl, userName);
    }

    public StatusProto.ResponseStatus createDatabase(String dbName, String locationUrl, String userName)
    {
        MetaProto.DbParam database = MetaProto.DbParam.newBuilder()
                .setDbName(dbName)
                .setLocationUrl(locationUrl)
                .setUserName(userName)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createDatabase(database);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create database status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createRegularTable(
            String dbName,
            String tblName,
            String userName,
            String storageFormatName,
            ArrayList<String> columnName,
            ArrayList<String> dataType)
    {
        String locationUrl = "";
        return createRegularTable(
                dbName,
                tblName,
                userName,
                locationUrl,
                storageFormatName,
                columnName,
                dataType);
    }

    public StatusProto.ResponseStatus createRegularTable(
            String dbName,
            String tblName,
            String userName,
            String locationUrl,
            String storageFormatName,
            ArrayList<String> columnName,
            ArrayList<String> dataType)
    {
        int tblType = 0;
        int columnNameSize = columnName.size();
        int dataTypeSize = dataType.size();
        StatusProto.ResponseStatus statusTable;
        if (columnNameSize == dataTypeSize) {
            ArrayList<MetaProto.ColParam> columns = new ArrayList<>();
            for (int i = 0; i < columnNameSize; i++) {
                MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                        .setColIndex(i)
                        .setDbName(dbName)
                        .setTblName(tblName)
                        .setColName(columnName.get(i))
                        .setColType(ColType.REGULAR.getColTypeId())
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
                    .setLocationUrl(locationUrl)
                    .setStorageFormatName(storageFormatName)
                    .setFiberFuncName("none")
                    .setFiberColId(-1)
                    .setColList(colList)
                    .build();
            try {
                statusTable = metaBlockingStub.createTable(table);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                StatusProto.ResponseStatus statusError = StatusProto.ResponseStatus.newBuilder()
                        .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                        .build();
                return statusError;
            }
        }
        else {
            statusTable = StatusProto.ResponseStatus.newBuilder()
                    .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                    .build();
        }
        logger.info("Create table status is : " + statusTable.getStatus());
        return statusTable;
    }

    public StatusProto.ResponseStatus createFiberTable(
            String dbName,
            String tblName,
            String userName,
            String storageFormatName,
            int fiberColIndex,
            String fiberFuncName,
            int timstampColIndex,
            ArrayList<String> columnName,
            ArrayList<String> dataType)
    {
        String locationUrl = "";
        return createFiberTable(
                dbName,
                tblName,
                userName,
                locationUrl,
                storageFormatName,
                fiberColIndex,
                fiberFuncName,
                timstampColIndex,
                columnName,
                dataType);
    }

    public StatusProto.ResponseStatus createFiberTable(
            String dbName,
            String tblName,
            String userName,
            String locationUrl,
            String storageFormatName,
            int fiberColIndex,
            String fiberFuncName,
            int timestampColIndex,
            ArrayList<String> columnName,
            ArrayList<String> dataType)
    {
        int tblType = 1;
        int columnNameSize = columnName.size();
        int dataTypeSize = dataType.size();
        StatusProto.ResponseStatus statusTable;
        if (columnNameSize == dataTypeSize) {
            ArrayList<MetaProto.ColParam> columns = new ArrayList<>();
            for (int i = 0; i < columnNameSize; i++) {
                MetaProto.ColParam column;
                if (i == timestampColIndex) {
                    column = MetaProto.ColParam.newBuilder()
                            .setColIndex(i)
                            .setDbName(dbName)
                            .setTblName(tblName)
                            .setColName(columnName.get(i))
                            .setColType(ColType.TIMESTAMP.getColTypeId())
                            .setDataType(dataType.get(i))
                            .build();
                }
                else {
                    column = MetaProto.ColParam.newBuilder()
                            .setColIndex(i)
                            .setDbName(dbName)
                            .setTblName(tblName)
                            .setColName(columnName.get(i))
                            .setColType(ColType.FIBER.getColTypeId())
                            .setDataType(dataType.get(i))
                            .build();
                }
                columns.add(column);
            }
            MetaProto.ColListType colList = MetaProto.ColListType.newBuilder()
                    .addAllColumn(columns)
                    .build();
            if (fiberColIndex >= columnNameSize) {
                System.err.println("FiberColIndex out of boundary!");
                statusTable = StatusProto.ResponseStatus.newBuilder()
                        .setStatus(StatusProto.ResponseStatus.State.CREATE_TABLE_ERROR)
                        .build();
            }
            else {
                MetaProto.TblParam table = MetaProto.TblParam.newBuilder()
                        .setDbName(dbName)
                        .setTblName(tblName)
                        .setUserName(userName)
                        .setTblType(tblType)
                        .setLocationUrl(locationUrl)
                        .setStorageFormatName(storageFormatName)
                        .setFiberColId(fiberColIndex)
                        .setFiberFuncName(fiberFuncName)
                        .setColList(colList)
                        .build();
                try {
                    statusTable = metaBlockingStub.createTable(table);
                }
                catch (StatusRuntimeException e) {
                    logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                    statusTable = StatusProto.ResponseStatus.newBuilder()
                            .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                            .build();
                    return statusTable;
                }
            }
        }
        else  {
            statusTable = StatusProto.ResponseStatus.newBuilder()
                    .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                    .build();
        }
        logger.info("Create table status is : " + statusTable.getStatus());
        return statusTable;
    }

    public MetaProto.StringListType listDatabases()
    {
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listDatabases(none);
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
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            column = MetaProto.ColParam.newBuilder().build();
            return column;
        }
        logger.info("Column is : " + column);
        return column;
    }

    public StatusProto.ResponseStatus renameColumn(String dbName,
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
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.renameColumn(renameColumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus renameTable(String dbName, String oldName, String newName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.RenameTblParam renameTable = MetaProto.RenameTblParam.newBuilder()
                .setDatabase(databaseName)
                .setOldName(oldName)
                .setNewName(newName)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.renameTable(renameTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Rename table status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus renameDatabase(String oldName, String newName)
    {
        MetaProto.RenameDbParam renameDatabase = MetaProto.RenameDbParam.newBuilder()
                .setOldName(oldName)
                .setNewName(newName)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.renameDatabase(renameDatabase);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Rename database status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus deleteTable(String dbName, String tblName)
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
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.deleteTable(databaseTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Delete table status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus deleteDatabase(String dbName)
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.deleteDatabase(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.DELETE_DATABASE_ERROR).build();
            return status;
        }
        logger.info("Delete database status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createDbParam(String dbName, String paramKey, String paramValue)
    {
        MetaProto.DbParamParam dbParam = MetaProto.DbParamParam.newBuilder()
                .setDbName(dbName)
                .setParamKey(paramKey)
                .setParamValue(paramValue)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createDbParam(dbParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create database param status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createTblParam(String dbName,
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
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createTblParam(tblParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create table param status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createTblPriv(String dbName,
                                              String tblName,
                                              String userName,
                                              int privType)
    {
        MetaProto.TblPrivParam tblPriv = MetaProto.TblPrivParam.newBuilder()
                .setDbName(dbName)
                .setTblName(tblName)
                .setUserName(userName)
                .setPrivType(privType)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createTblPriv(tblPriv);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create tblpriv status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createStorageFormat(String storageFormatName,
                                                    String compression,
                                                    String serialFormat)
    {
        MetaProto.StorageFormatParam storageFormat
                = MetaProto.StorageFormatParam.newBuilder()
                .setStorageFormatName(storageFormatName)
                .setCompression(compression)
                .setSerialFormat(serialFormat)
                .build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createStorageFormat(storageFormat);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create storage format status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createFiberFunc(String fiberFuncName, byte[] fiberFuncContent)
    {
        ByteString byteString = ByteString.copyFrom(fiberFuncContent);
        MetaProto.FiberFuncParam fiberFunc = MetaProto.FiberFuncParam.newBuilder()
                .setFiberFuncName(fiberFuncName)
                .setFiberFuncContent(byteString).build();
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createFiberFunc(fiberFunc);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
            return status;
        }
        logger.info("Create fiber function status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createBlockIndex(String dbName,
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
        StatusProto.ResponseStatus status;
        try {
            status = metaBlockingStub.createBlockIndex(blockIndex);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = StatusProto.ResponseStatus.newBuilder().build();
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
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Filter block paths is : " + stringList);
        return stringList;
    }

    public void stopServer()
    {
        MetaProto.NoneType noneType =
                MetaProto.NoneType.newBuilder().build();
        try {
            metaBlockingStub.stopServer(noneType);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }
}
