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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// todo add security mechanism for rpc communication
// todo alice: add name pattern constraint
//             name [a-zA-Z0-9]
//             name and password length
//             url
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

    public void shutdownNow()
    {
        this.channel.shutdownNow();
    }

    private boolean nameValidate(String userName)
    {
        String regEx = "^[a-zA-Z][a-zA-Z0-9]*$";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(userName);
        return m.find();
    }

    private boolean lengthValidate(String name)
    {
        int length = name.length();
        boolean len = true;
        if (length > 50) {
            len = false;
        }
        return len;
    }

    private boolean urlValidate(String url)
    {
//        String regEx = "^[a-zA-Z]+://(([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3})|[a-zA-Z0-9]+)(:[0-9]{0,5})?(/[a-zA-Z]+)+$";
//        Pattern p = Pattern.compile(regEx);
//        Matcher m = p.matcher(url);
//        return m.find();
        return true;
    }

    public StatusProto.ResponseStatus createUser(String userName, String password)
    {
        boolean nameFormat = nameValidate(userName);
        boolean nameLen = lengthValidate(userName);
        boolean passLen = lengthValidate(password);
        StatusProto.ResponseStatus status;
        if (nameFormat & nameLen & passLen) {
            MetaProto.UserParam user =
                    MetaProto.UserParam.newBuilder()
                            .setUserName(userName)
                            .setPassword(password)
                            .build();
            try {
                status = metaBlockingStub.createUser(user);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
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
        boolean dbNameFormat = nameValidate(dbName);
        boolean userNameFormat = nameValidate(userName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean userNameLen = lengthValidate(userName);
        boolean locationUrlFormat = true;
        if (!locationUrl.equals("")) {
            locationUrlFormat = urlValidate(locationUrl);
        }
        StatusProto.ResponseStatus status;
        if (dbNameFormat & userNameFormat & dbNameLen & userNameLen & locationUrlFormat) {
            MetaProto.DbParam database = MetaProto.DbParam.newBuilder()
                    .setDbName(dbName)
                    .setLocationUrl(locationUrl)
                    .setUserName(userName)
                    .build();
            try {
                status = metaBlockingStub.createDatabase(database);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create database status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createRegularTable(
            String dbName,
            String tblName,
            String userName,
            String storageFormatName,
            List<String> columnName,
            List<String> dataType)
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
            List<String> columnName,
            List<String> dataType)
    { //todo datatype should write to a enum type??????
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        boolean userNameFormat = nameValidate(userName);
        boolean userNameLen = lengthValidate(userName);
        boolean locationUrlFormat = urlValidate(locationUrl);
        boolean storageFormatNameFormat = nameValidate(storageFormatName);
        boolean storageFormatNameLen = lengthValidate(storageFormatName);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen
                & userNameFormat & userNameLen
                & locationUrlFormat
                & storageFormatNameFormat & storageFormatNameLen) {
            int tblType = 0;
            int columnNameSize = columnName.size();
            int dataTypeSize = dataType.size();
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
                        .setFuncName("none")
                        .setFiberColId(-1)
                        .setColList(colList)
                        .build();
                try {
                    status = metaBlockingStub.createTable(table);
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
                status = StatusProto.ResponseStatus.newBuilder()
                        .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                        .build();
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder()
                    .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                    .build();
        }
        logger.info("Create table status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createFiberTable(
            String dbName,
            String tblName,
            String userName,
            String storageFormatName,
            int fiberColIndex,
            String funcName,
            int timstampColIndex,
            List<String> columnName,
            List<String> dataType)
    {
        String locationUrl = "";
        return createFiberTable(
                dbName,
                tblName,
                userName,
                locationUrl,
                storageFormatName,
                fiberColIndex,
                funcName,
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
            String funcName,
            int timestampColIndex,
            List<String> columnName,
            List<String> dataType)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        boolean userNameFormat = nameValidate(userName);
        boolean userNameLen = lengthValidate(userName);
        boolean locationUrlFormat = urlValidate(locationUrl);
        boolean storageFormatNameFormat = nameValidate(storageFormatName);
        boolean storageFormatNameLen = lengthValidate(storageFormatName);
        boolean funcNameFormat = nameValidate(funcName);
        boolean funcNameLen = lengthValidate(funcName);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen
                & userNameFormat & userNameLen
                & locationUrlFormat
                & storageFormatNameFormat & storageFormatNameLen
                & funcNameFormat & funcNameLen) {
            int tblType = 1;
            int columnNameSize = columnName.size();
            int dataTypeSize = dataType.size();
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
                    status = StatusProto.ResponseStatus.newBuilder()
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
                            .setFuncName(funcName)
                            .setColList(colList)
                            .build();
                    try {
                        status = metaBlockingStub.createTable(table);
                    }
                    catch (StatusRuntimeException e) {
                        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                        status = StatusProto.ResponseStatus.newBuilder()
                                .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                                .build();
                        return status;
                    }
                }
            }
            else {
                status = StatusProto.ResponseStatus.newBuilder()
                        .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                        .build();
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder()
                    .setStatus(StatusProto.ResponseStatus.State.CREAT_COLUMN_ERROR)
                    .build();
        }
        logger.info("Create table status is : " + status.getStatus());
        return status;
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
        logger.info("Databases list : " + stringList);
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
        logger.info("Tables list : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType listColumns(String dbName, String tblName)
    {
        MetaProto.DbNameParam dbNameParam = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tblNameParam = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.DbTblParam dbTblParam = MetaProto.DbTblParam.newBuilder()
                .setDatabase(dbNameParam)
                .setTable(tblNameParam)
                .build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listColumns(dbTblParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Columns list : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType listColumnsDataType(String dbName, String tblName)
    {
        MetaProto.DbNameParam dbNameParam = MetaProto.DbNameParam.newBuilder()
                .setDatabase(dbName)
                .build();
        MetaProto.TblNameParam tblNameParam = MetaProto.TblNameParam.newBuilder()
                .setTable(tblName)
                .build();
        MetaProto.DbTblParam dbTblParam = MetaProto.DbTblParam.newBuilder()
                .setDatabase(dbNameParam)
                .setTable(tblNameParam)
                .build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listColumnsDataType(dbTblParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("ColumnsDataType list : " + stringList);
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
            database = MetaProto.DbParam.newBuilder().setIsEmpty(false).build();
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

    public StatusProto.ResponseStatus createDbParam(String dbName,
                                                    String paramKey,
                                                    String paramValue)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen) {
            MetaProto.DbParamParam dbParam = MetaProto.DbParamParam.newBuilder()
                    .setDbName(dbName)
                    .setParamKey(paramKey)
                    .setParamValue(paramValue)
                    .build();
            try {
                status = metaBlockingStub.createDbParam(dbParam);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create database param status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createTblParam(String dbName,
                                               String tblName,
                                               String paramKey,
                                               String paramValue)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen) {
            MetaProto.TblParamParam tblParam = MetaProto.TblParamParam.newBuilder()
                    .setDbName(dbName)
                    .setTblName(tblName)
                    .setParamKey(paramKey)
                    .setParamValue(paramValue)
                    .build();
            try {
                status = metaBlockingStub.createTblParam(tblParam);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create table param status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createTblPriv(String dbName,
                                              String tblName,
                                              String userName,
                                              int privType)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        boolean userNameFormat = nameValidate(userName);
        boolean userNameLen = lengthValidate(userName);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen
                & userNameFormat & userNameLen) {
            MetaProto.TblPrivParam tblPriv = MetaProto.TblPrivParam.newBuilder()
                    .setDbName(dbName)
                    .setTblName(tblName)
                    .setUserName(userName)
                    .setPrivType(privType)
                    .build();
            try {
                status = metaBlockingStub.createTblPriv(tblPriv);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create tblpriv status is : " + status.getStatus());
        return status;
    }

    public StatusProto.ResponseStatus createStorageFormat(String storageFormatName,
                                                    String compression,
                                                    String serialFormat)
    {
        boolean storageFormatNameFormat = nameValidate(storageFormatName);
        boolean storageFormatNameLen = lengthValidate(storageFormatName);
        StatusProto.ResponseStatus status;
        if (storageFormatNameFormat & storageFormatNameLen) {
            MetaProto.StorageFormatParam storageFormat
                    = MetaProto.StorageFormatParam.newBuilder()
                    .setStorageFormatName(storageFormatName)
                    .setCompression(compression)
                    .setSerialFormat(serialFormat)
                    .build();
            try {
                status = metaBlockingStub.createStorageFormat(storageFormat);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create storage format status is : " + status.getStatus());
        return status;
    }

    // todo get storage format

//    public StatusProto.ResponseStatus createFiberFunc(String fiberFuncName, SerializableFunction fiberFunc)
//    {
//        StatusProto.ResponseStatus status;
//        try {
//            ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            ObjectOutput objOutput = new ObjectOutputStream(bos);
//            objOutput.writeObject(fiberFunc);
//            objOutput.flush();
//            ByteString byteString = ByteString.copyFrom(bos.toByteArray());
//            MetaProto.FiberFuncParam fiberFuncParam = MetaProto.FiberFuncParam.newBuilder()
//                    .setFiberFuncName(fiberFuncName)
//                    .setFiberFuncContent(byteString).build();
//            status = metaBlockingStub.createFiberFunc(fiberFuncParam);
    public MetaProto.StorageFormatParam getStorageFormat(String storageFormatName)
    {
        MetaProto.StorageFormatParam storageFormat;
        if (nameValidate(storageFormatName)) {
            MetaProto.GetStorageFormatParam getStorageFormatParam =
                    MetaProto.GetStorageFormatParam.newBuilder()
                    .setStorageFormatName(storageFormatName)
                    .build();
            try {
                storageFormat = metaBlockingStub.getStorageFormat(getStorageFormatParam);
            }
            catch (StatusRuntimeException e) {
                storageFormat = MetaProto.StorageFormatParam.newBuilder().setIsEmpty(true).build();
                return storageFormat;
            }
        }
        else {
            storageFormat = MetaProto.StorageFormatParam.newBuilder().setIsEmpty(true).build();
        }
        logger.info("Create storage format status is : " + storageFormat);
        return storageFormat;
    }

    public StatusProto.ResponseStatus createFunc(String funcName,
                                                      byte[] funcContent)
    {
        boolean funcNameFormat = nameValidate(funcName);
        boolean funcNameLen = lengthValidate(funcName);
        StatusProto.ResponseStatus status;
        if (funcNameFormat & funcNameLen) {
            ByteString byteString = ByteString.copyFrom(funcContent);
            MetaProto.FuncParam func = MetaProto.FuncParam.newBuilder()
                    .setFuncName(funcName)
                    .setFuncContent(byteString).build();
            try {
                status = metaBlockingStub.createFunc(func);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create fiber function status is : " + status.getStatus());
        return status;
    }

//    public SerializableFunction getFiberFunc(String fiberFuncName)
//    {
//        MetaProto.GetFiberFuncParam getFiberFuncParam
//                = MetaProto.GetFiberFuncParam.newBuilder()
//                .setFiberFuncName(fiberFuncName)
//                .build();
//        SerializableFunction function = null;
//        try {
//            MetaProto.FiberFuncParam fiberFuncParam = metaBlockingStub.getFiberFunc(getFiberFuncParam);
//            ObjectInputStream ois = new ObjectInputStream(fiberFuncParam.getFiberFuncContent().newInput());
//            function = (SerializableFunction) ois.readObject();
//            ois.close();
//        }
//        catch (StatusRuntimeException e) {
//            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
//        }
//        catch (IOException | ClassNotFoundException e) {
//            logger.log(Level.WARNING, "DeSerialization failed");
//        }
//        return function;
    public MetaProto.FuncParam getFunc(String funcName)
    {
        MetaProto.FuncParam funcParam;
        if (nameValidate(funcName)) {
            MetaProto.GetFuncParam getFuncParam =
                    MetaProto.GetFuncParam.newBuilder()
                    .setFuncName(funcName)
                    .build();
            try {
                funcParam = metaBlockingStub.getFunc(getFuncParam);
            }
            catch (StatusRuntimeException e) {
                funcParam = MetaProto.FuncParam.newBuilder().build();
                return funcParam;
            }
        }
        else {
            funcParam = MetaProto.FuncParam.newBuilder().setIsEmpty(true).build();
        }
        logger.info("Create storage format status is : " + funcParam);
        return funcParam;
    }

    public StatusProto.ResponseStatus createBlockIndex(String dbName,
                                                 String tblName,
                                                 int value,
                                                 long timeBegin,
                                                 long timeEnd,
                                                 String path)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        boolean pathFormat = urlValidate(path);
        StatusProto.ResponseStatus status;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen
                & pathFormat) {
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
            try {
                status = metaBlockingStub.createBlockIndex(blockIndex);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                status = StatusProto.ResponseStatus.newBuilder().build();
                return status;
            }
        }
        else {
            status = StatusProto.ResponseStatus.newBuilder().build();
        }
        logger.info("Create block index status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StringListType filterBlockIndex(String dbName,
                                                     String tblName,
                                                     long timeBegin,
                                                     long timeEnd)
    {
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        MetaProto.StringListType stringList;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen) {
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
            try {
                stringList = metaBlockingStub.filterBlockIndex(filterBlockIndex);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                stringList = MetaProto.StringListType.newBuilder().build();
                return stringList;
            }
        }
        else {
            stringList = MetaProto.StringListType.newBuilder().build();
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
        boolean dbNameFormat = nameValidate(dbName);
        boolean dbNameLen = lengthValidate(dbName);
        boolean tblNameFormat = nameValidate(tblName);
        boolean tblNameLen = lengthValidate(tblName);
        MetaProto.StringListType stringList;
        if (dbNameFormat & dbNameLen
                & tblNameFormat & tblNameLen) {
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
            try {
                stringList = metaBlockingStub.filterBlockIndexByFiber(filterBlockIndexByFiber);
            }
            catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                stringList = MetaProto.StringListType.newBuilder().build();
                return stringList;
            }
        }
        else {
            stringList = MetaProto.StringListType.newBuilder().build();
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
