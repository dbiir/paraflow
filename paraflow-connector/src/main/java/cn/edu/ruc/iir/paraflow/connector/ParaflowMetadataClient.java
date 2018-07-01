package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.edu.ruc.iir.paraflow.commons.proto.StatusProto.ResponseStatus.State.STATUS_OK;
import static java.util.Objects.requireNonNull;

public class MetaDataQuery
{
    private static final Logger log = Logger.get(MetaDataQuery.class);
    private static final Map<String, String> sqlTable = new HashMap<>();
    MetaClient metaClient = new MetaClient("127.0.0.1", 10012);

    private FileSystem fileSystem;

    // read config. check if meta table already exists in database, or else initialize tables.
    public MetaDataQuery(FSFactory fsFactory)
    {
        fileSystem = fsFactory.getFS().get();
    }

    public List<String> getAllDatabases()
    {
        MetaProto.StringListType stringList = metaClient.listDatabases();
        log.debug("Get all databases");
        List<String> resultL = new ArrayList<>();
        int count = stringList.getStrCount();
        for (int i = 0; i < count; i++) {
            resultL.add(stringList.getStr(i));
        }
        return resultL;
    }

    private String getDatabaseId(String db)
    {
        log.debug("Get database " + db);
        MetaProto.DbParam dbParam = metaClient.getDatabase(db);

        if (dbParam.getIsEmpty()) {
            log.debug("Find no database with name " + db);
            return null;
        }

        return dbParam.getDbName();
    }

    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        log.info("List all tables with prefix " + prefix.toString());
        List<SchemaTableName> tables = new ArrayList<>();
        String dbPrefix = prefix.getSchemaName();
        log.debug("listTables dbPrefix: " + dbPrefix);
        String tblPrefix = prefix.getTableName();
        log.debug("listTables tblPrefix: " + tblPrefix);

        // if dbPrefix not mean to match all
        String tblName;
        String dbName;
        if (dbPrefix != null) {
            if (tblPrefix != null) {
                tblName = tblPrefix;
                dbName = dbPrefix;
            }
            else {
                MetaProto.StringListType stringListType = metaClient.listTables(dbPrefix);
                log.info("record size: " + stringListType.getStrCount());
                if (stringListType.getStrCount() == 0) {
                    return tables;
                }
                for (int i = 0; i < stringListType.getStrCount(); i++) {
                    tblName = stringListType.getStr(0);
                    dbName = dbPrefix;
                    log.debug("listTables tableName: " + formName(dbName, tblName));
                    tables.add(new SchemaTableName(dbName, tblName));
                }
            }
        }
        return tables;
    }

    private String getTableId(String dbName, String tblName)
    {
        MetaProto.TblParam tblParam = metaClient.getTable(dbName, tblName);
        if (tblParam.getIsEmpty()) {
            return null;
        }
        return tblParam.getTblId();
    }

//    private Optional<ParaflowTable> getTable(String databaseName, String tableName)
//    {
//        log.debug("Get table " + formName(databaseName, tableName));
//        ParaflowTableHandle table;
//        ParaflowTableLayoutHandle tableLayout;
//        List<ParaflowColumnHandle> columns;
//        List<ColumnMetadata> metadatas;
//
//        Optional<ParaflowTableHandle> tableOptional = getTableHandle(databaseName, tableName);
//        if (!tableOptional.isPresent()) {
//            log.warn("Table not exists");
//            return Optional.empty();
//        }
//        table = tableOptional.get();
//
//        Optional<ParaflowTableLayoutHandle> tableLayoutOptional = getTableLayout(databaseName, tableName);
//        if (!tableLayoutOptional.isPresent()) {
//            log.warn("Table layout not exists");
//            return Optional.empty();
//        }
//        tableLayout = tableLayoutOptional.get();
//
//        Optional<List<ParaflowColumnHandle>> columnsOptional = getTableColumnHandle(databaseName, tableName);
//        if (!columnsOptional.isPresent()) {
//            log.warn("Column handles not exists");
//            return Optional.empty();
//        }
//        columns = columnsOptional.get();
//
//        Optional<List<ColumnMetadata>> metadatasOptional = getTableColMetadata(databaseName, tableName);
//        if (!metadatasOptional.isPresent()) {
//            log.warn("Column metadata not exists");
//            return Optional.empty();
//        }
//        metadatas = metadatasOptional.get();
//
//        ParaflowTable hdfsTable = new ParaflowTable(table, tableLayout, columns, metadatas);
//        return Optional.of(hdfsTable);
//    }

    @Override
    public Optional<ParaflowTableHandle> getTableHandle(String connectorId, String dbName, String tblName)
    {
        log.debug("Get table handle " + formName(dbName, tblName));
        ParaflowTableHandle table;
        MetaProto.TblParam tblParam = metaClient.getTable(dbName, tblName);
        if (tblParam.getIsEmpty()) {
            log.error("Match more/less than one table");
            return Optional.empty();
        }
        String tblName = tblParam.getTblName();
        String dbName = tblParam.getDbName();
        String location = tblParam.getLocationUrl();
        table = new ParaflowTableHandle(
                requireNonNull(connectorId, "connectorId is null"),
                requireNonNull(dbName, "database name is null"),
                requireNonNull(tblName, "table name is null"),
                requireNonNull(location, "location uri is null"));
        return Optional.of(table);
    }

    @Override
    public Optional<ParaflowTableLayoutHandle> getTableLayout(String connectorId, String dbName, String tblName)
    {
        log.debug("Get table layout " + formName(dbName, tblName));
        ParaflowTableLayoutHandle tableLayout;
        MetaProto.TblParam tblParam = metaClient.getTable(dbName, tblName);
        if (tblParam.getIsEmpty()) {
            log.error("Match more/less than one table");
            return Optional.empty();
        }
        ParaflowTableHandle tableHandle = getTableHandle(connectorId, dbName, tblName).orElse(null);
        if (tableHandle == null) {
            log.error("Match no table handle");
            return Optional.empty();
        }

        String fiberColName = tblParam.getColList(tblParam.getFiberColId());
        String timeColName = String.valueOf(tblParam.getCreateTime());
        String fiberFunc = tblParam.getFuncName();
        Function function = parseFunction(fiberFunc);
        if (function == null) {
            log.error("Function parse error");
            return Optional.empty();
        }

        if (fiberColName.equals("") || timeColName.equals("")) {
            tableLayout = new ParaflowTableLayoutHandle(tableHandle);
        }
        else {
            // construct ColumnHandle
            ParaflowColumnHandle fiberCol = getColumnHandle(connectorId, fiberColName, tblName, dbName);
            ParaflowColumnHandle timeCol = getColumnHandle(connectorId, timeColName, tblName, dbName);
            tableLayout = new ParaflowTableLayoutHandle(tableHandle, fiberCol, timeCol, function, StorageFormat.PARQUET, Optional.empty());
        }
        return Optional.of(tableLayout);
    }

    /**
     * Get all column handles of specified table
     * */
    @Override
    public Optional<List<ParaflowColumnHandle>> getTableColumnHandle(String connectorId, String dbName, String tblName)
    {
        log.debug("Get list of column handles of table " + formName(dbName, tblName));
        List<ParaflowColumnHandle> columnHandles = new ArrayList<>();
        String colName;
        String colTypeName;
        String dataTypeName;
        MetaProto.StringListType listColumns = metaClient.listColumns(dbName, tblName);
        if (listColumns.getIsEmpty()) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        for (int i = 0; i < listColumns.getStrCount(); i++) {
            colName = listColumns.getStr(i);
            MetaProto.ColParam colParam = metaClient.getColumn(dbName, tblName, colName);
            colTypeName = String.valueOf(colParam.getColType());
            dataTypeName = String.valueOf(colParam.getDataType());
            // Deal with col type
            ParaflowColumnHandle.ColumnType colType = getColType(colTypeName);
            // Deal with data type
            Type type = getType(dataTypeName);
            columnHandles.add(new ParaflowColumnHandle(colName, type, "", colType, connectorId));
        }
        return Optional.of(columnHandles);
    }

    @Override
    public void shutdown()
    {
    }

    private ParaflowColumnHandle getColumnHandle(String connectorId, String colName, String tblName, String dbName)
    {
        MetaProto.ColParam colParam = metaClient.getColumn(dbName, tblName, colName);
        if (colParam.getIsEmpty()) {
            log.error("Match more/less than one column");
        }
        String colTypeName = colParam.getColType();
        String dataType = colParam.getDataType();
        // Deal with colType
        ParaflowColumnHandle.ColumnType colType = getColType(colTypeName);
        // Deal with type
        Type type = getType(dataType);
        return new ParaflowColumnHandle(colName, type, "", colType, connectorId);
    }

    public Optional<List<ColumnMetadata>> getTableColMetadata(String connectorId, String dbName, String tblName)
    {
        log.debug("Get list of column metadata of table " + formName(dbName, tblName));
        List<ColumnMetadata> colMetadatas = new ArrayList<>();
        MetaProto.StringListType dataTypeList = metaClient.listColumnsDataType(dbName, tblName);
        MetaProto.StringListType colNameList = metaClient.listColumns(dbName, tblName);
        if (dataTypeList.getIsEmpty() || colNameList.getIsEmpty()) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        for (int i = 0; i < dataTypeList.getStrCount(); i++) {
            String dataType = dataTypeList.getStr(i);
            Type type = getType(dataType);
            ColumnMetadata metadata = new ColumnMetadata(
                    colNameList.getStr(i),
                    type,
                    "",
                    false);
            colMetadatas.add(metadata);
        }
        return Optional.of(colMetadatas);
    }

//    private ColumnMetadata getColMetadata(String colName, String tblName, String dbName)
//    {
//        log.debug("Get metadata of col " + formName(dbName, tblName, colName));
//        List<JDBCRecord> records;
//        String sql = "SELECT type FROM cols WHERE name='"
//                + colName
//                + "' AND tbl_name='"
//                + tblName
//                + " AND db_name='"
//                + dbName
//                + "';";
//        String[] colFields = {"type"};
//        records = jdbcDriver.executreQuery(sql, colFields);
//        if (records.size() != 1) {
//            log.error("Match more/less than one table");
//            throw new RecordMoreLessException();
//        }
//        JDBCRecord colRecord = records.get(0);
//        String typeName = colRecord.getString(colFields[0]);
//        Type type = getType(typeName);
//        if (type == UnknownType.UNKNOWN) {
//            log.error("Type unknown!");
//            throw new TypeUnknownException();
//        }
//        return new ColumnMetadata(colName, type, "", false);
//    }

    @Override
    public void createDatabase(ConnectorSession session, ParaflowDatabase database)
    {
        createDatabase(database);
    }

    private void createDatabase(ParaflowDatabase database)
    {
        database.setLocation(formPath(database.getName()).toString());
        log.debug("Create database " + database.getName());
        createDatabase(database.getName(),
                database.getLocation());
    }

    private void createDatabase(String dbName, String dbPath)
    {
        StatusProto.ResponseStatus responseStatus = metaClient.createDatabase(dbName, dbPath, " "); //????????????????????????
        if (responseStatus.getStatus() == STATUS_OK) {
            log.error("Create database" + dbName + " successed!");
        }
        else {
            log.error("Create database" + dbName + " failed!");
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        log.debug("Create table " + tableMetadata.getTable().getTableName());
        String tblName = tableMetadata.getTable().getTableName();
        String dbName = tableMetadata.getTable().getSchemaName();
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        List<String> columnName = new LinkedList<>();
        List<String> dataType = new LinkedList<>();
        for (ColumnMetadata column : columns) {
            columnName.add(column.getName());
            dataType.add(column.getType().getDisplayName());
        }
        String userName = "";
        String storageFormatName = "";
        metaClient.createRegularTable(dbName, tblName, userName, storageFormatName, columnName, dataType);
    }

    @Override
    public void createTableWithFiber(ConnectorSession session, ConnectorTableMetadata tableMetadata, String fiberKey, String function, String timeKey)
    {
        log.debug("Create table with fiber " + tableMetadata.getTable().getTableName());
        // check fiberKey, function and timeKey
        List<ColumnMetadata> columns = tableMetadata.getColumns();
//        List<String> columnNames = columns.stream()
//                .map(ColumnMetadata::getName)
//                .collect(Collectors.toList());
        List<String> columnName = new LinkedList<>();
        List<String> dataType = new LinkedList<>();
        for (ColumnMetadata column : columns) {
            columnName.add(column.getName());
            dataType.add(column.getType().getDisplayName());
        }

        String tblName = tableMetadata.getTable().getTableName();
        String dbName = tableMetadata.getTable().getSchemaName();
        String storageFormatName = "";
        String userName = "";
        int fiberColIndex = Integer.parseInt(fiberKey);
        int timstampColIndex = Integer.parseInt(timeKey);
        // createTable
        metaClient.createFiberTable(dbName, tblName, userName,
                storageFormatName, fiberColIndex, function,
                timstampColIndex, columnName, dataType);
    }

//    private void createTable(List<ColumnMetadata> columns, String dbId, String name, String dbName, String location, String storage, String fiberKey, String function, String timeKey)
//    {
//        StringBuilder sql = new StringBuilder();
//        sql.append("INSERT INTO tbls(db_id, name, db_name, location, storage, fib_k, fib_func, time_k) VALUES('")
//                .append(dbId)
//                .append("', '")
//                .append(name)
//                .append("', '")
//                .append(dbName)
//                .append("', '")
//                .append(location)
//                .append("', '")
//                .append(storage)
//                .append("', '")
//                .append(fiberKey)
//                .append("', '")
//                .append(function)
//                .append("', '")
//                .append(timeKey)
//                .append("');");
//        if (jdbcDriver.executeUpdate(sql.toString()) != 0) {
//            try {
//                log.debug("Create hdfs dir for " + formName(dbName, name));
//                fileSystem.mkdirs(formPath(dbName, name));
//            }
//            catch (IOException e) {
//                log.debug("Error sql: " + sql.toString());
//                log.error(e);
//                // TODO exit ?
//            }
//        }
//        else {
//            log.error("Create table " + formName(dbName, name) + " failed!");
//        }
//
//        String tableId = getTableId(dbName, name);
//
//        // add cols information
//        for (ColumnMetadata col : columns) {
//            String colType = "regular";
//            if (Objects.equals(col.getName(), fiberKey)) {
//                colType = "fiber";
//            }
//            if (Objects.equals(col.getName(), timeKey)) {
//                colType = "time";
//            }
//            sql.delete(0, sql.length() - 1);
//            sql.append("INSERT INTO cols(name, tbl_id, tbl_name, db_name, data_type, col_type) VALUES('")
//                    .append(col.getName())
//                    .append("', '")
//                    .append(tableId)
//                    .append("', '")
//                    .append(name)
//                    .append("', '")
//                    .append(dbName)
//                    .append("', '")
//                    .append(col.getType())
//                    .append("', '")
//                    .append(colType)
//                    .append("');");
//            if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
//                log.debug("Error sql: " + sql.toString());
//                log.error("Create cols for table " + formName(dbName, name) + " failed!");
//                // TODO exit ?
//            }
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void deleteTable(String database, String table)
//    {
//        StringBuilder sb1 = new StringBuilder();
//        sb1.append("DELETE FROM tbls WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(table)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb1.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//        StringBuilder sb2 = new StringBuilder();
//        sb2.append("DELETE FROM cols WHERE db_name='")
//                .append(database)
//                .append("' AND tbl_name='")
//                .append(table)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb2.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void deleteDatabase(String database)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("DELETE FROM dbs WHERE name='")
//                .append(database)
//                .append("';");
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//        System.out.println(sb.toString());
//
//        StringBuilder sb2 = new StringBuilder();
//        sb2.append("DELETE FROM tbls WHERE db_name='")
//                .append(database)
//                .append("';");
//        if (jdbcDriver.executeUpdate(sb2.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//        System.out.println(sb2.toString());
//
//        StringBuilder sb3 = new StringBuilder();
//        sb3.append("DELETE FROM cols WHERE db_name='")
//                .append(database)
//                .append("';");
//        System.out.println(sb3.toString());
//        if (jdbcDriver.executeUpdate(sb3.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void renameTable(String database, String oldName, String newName)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("UPDATE tbls SET name='")
//                .append(newName)
//                .append("' WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(oldName)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void renameDatabase(String oldName, String newName)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("UPDATE dbs SET name='")
//                .append(newName)
//                .append("' WHERE name='")
//                .append(oldName)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void renameColumn(String database, String table, String oldName, String newName)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("UPDATE cols SET name='")
//                .append(newName)
//                .append("' WHERE db_name='")
//                .append(database)
//                .append("' AND tbl_name='")
//                .append(table)
//                .append("' AND name='")
//                .append(oldName)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            System.out.println("Error sql: " + sb.toString());
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void createFiber(String database, String table, long value)
//    {
//        StringBuilder sb1 = new StringBuilder();
//        StringBuilder sb2 = new StringBuilder();
//        sb1.append("SELECT id FROM tbls WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(table)
//                .append("';");
//        List<Long> resultL = new ArrayList<>();
//        String[] fields = {"id"};
//        List<JDBCRecord> records = jdbcDriver.executeQuery(sb1.toString(), fields);
//        records.forEach(record -> resultL.add(record.getLong(fields[0])));
//        Long tblId;
//        tblId = resultL.get(0);
//        sb2.append("INSERT INTO fibers(tbl_id,fiber_v) VALUES(")
//                .append(tblId)
//                .append(",")
//                .append(value)
//                .append(");");
//        if (jdbcDriver.executeUpdate(sb2.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void updateFiber(String database, String table, long oldV, long newV)
//    {
//        StringBuilder sb1 = new StringBuilder();
//        StringBuilder sb2 = new StringBuilder();
//        sb1.append("SELECT id FROM tbls WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(table)
//                .append("';");
//        List<Long> resultL = new ArrayList<>();
//        String[] fields = {"id"};
//        List<JDBCRecord> records = jdbcDriver.executeQuery(sb1.toString(), fields);
//        records.forEach(record -> resultL.add(record.getLong(fields[0])));
//        Long tblId;
//        tblId = resultL.get(0);
//        sb2.append("UPDATE fibers SET fiber_v=")
//                .append(newV)
//                .append("WHERE tbl_id=")
//                .append(tblId)
//                .append(" AND fiber_v=")
//                .append(oldV)
//                .append(";");
//
//        if (jdbcDriver.executeUpdate(sb2.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void deleteFiber(String database, String table, long value)
//    {
//        StringBuilder sb1 = new StringBuilder();
//        StringBuilder sb2 = new StringBuilder();
//        sb1.append("SELECT id FROM tbls WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(table)
//                .append("';");
//        List<Long> resultL = new ArrayList<>();
//        String[] fields = {"id"};
//        List<JDBCRecord> records = jdbcDriver.executeQuery(sb1.toString(), fields);
//        records.forEach(record -> resultL.add(record.getLong(fields[0])));
//        Long tblId;
//        tblId = resultL.get(0);
//        sb2.append("DELETE FROM fibers WHERE tbl_id=")
//                .append(tblId)
//                .append(" AND fiber_v=")
//                .append(value)
//                .append(";");
//
//        if (jdbcDriver.executeUpdate(sb2.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public List<Long> getFibers(String database, String table)
//    {
//        StringBuilder sb1 = new StringBuilder();
//        StringBuilder sb2 = new StringBuilder();
//        sb1.append("SELECT id FROM tbls WHERE db_name='")
//                .append(database)
//                .append("' AND name='")
//                .append(table)
//                .append("';");
//        List<Long> resultL1 = new ArrayList<>();
//        String[] fields1 = {"id"};
//        List<JDBCRecord> records1 = jdbcDriver.executeQuery(sb1.toString(), fields1);
//        records1.forEach(record -> resultL1.add(record.getLong(fields1[0])));
//        Long tblId;
//        tblId = resultL1.get(0);
//        sb2.append("SELECT fiber_v FROM fibers WHERE tbl_id=")
//                .append(tblId)
//                .append(";");
//        List<Long> resultL = new ArrayList<>();
//        String[] fields = {"fiber_v"};
//        List<JDBCRecord> records = jdbcDriver.executeQuery(sb2.toString(), fields);
//        records.forEach(record -> resultL.add(record.getLong(fields[0])));
//        return resultL;
//    }
//
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void addBlock(long fiberId, String beginT, String endT, String path)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("INSERT INTO fiberfiles(fiber_id,time_b,time_e,path) VALUES(")
//                .append(fiberId)
//                .append(",'")
//                .append(beginT)
//                .append("','")
//                .append(endT)
//                .append("','")
//                .append(path)
//                .append("');");
//
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public void deleteBlock(long fiberId, String path)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("DELETE FROM fiberfiles WHERE fiber_id=")
//                .append(fiberId)
//                .append(" AND path='")
//                .append(path)
//                .append("';");
//
//        if (jdbcDriver.executeUpdate(sb.toString()) == 0) {
//            log.debug("Error sql: \" + sql.toString()");
//        }
//    }
//    /**
//     * Currently unsupported in SQL client for safety.
//     * Used for unit test only
//     * */
//    public List<String> getBlocks(long fiberId, String beginT, String endT)
//    {
//        StringBuilder sb = new StringBuilder();
//        sb.append("SELECT path FROM fiberfiles WHERE fiber_id='")
//                .append(fiberId)
//                .append("' AND time_b='")
//                .append(beginT)
//                .append("' AND time_e='")
//                .append(endT)
//                .append("';");
//        List<String> resultL = new ArrayList<>();
//        String[] fields = {"path"};
//        List<JDBCRecord> records = jdbcDriver.executeQuery(sb.toString(), fields);
//        records.forEach(record -> resultL.add(record.getString(fields[0])));
//        return resultL;
//    }
//
//    /**
//     * Filter blocks
//     * */
//    @Override
    public List<String> filterBlocks(String db, String table, Optional<Long> fiberId, Optional<Long> timeLow, Optional<Long> timeHigh)
    {
        MetaProto.StringListType stringListType = metaClient.filterBlockIndexByFiber(db, table, Integer.parseInt(String.valueOf(fiberId.get())), timeLow.get(), timeHigh.get());
        List<String> resultL = new ArrayList<>();
        for (int i = 0; i < stringListType.getStrCount(); i++) {
            resultL.add(stringListType.getStr(i));
        }
        return resultL;
    }

    private ParaflowColumnHandle.ColumnType getColType(String typeName)
    {
        log.debug("Get col type " + typeName);
        switch (typeName.toUpperCase()) {
            case "FIBER": return ParaflowColumnHandle.ColumnType.FIBER_COL;
            case "TIME": return ParaflowColumnHandle.ColumnType.TIME_COL;
            case "REGULAR": return ParaflowColumnHandle.ColumnType.REGULAR;
            default : log.error("No match col type!");
                return ParaflowColumnHandle.ColumnType.NOTVALID;
        }
    }

    private Type getType(String typeName)
    {
        log.debug("Get type " + typeName);
        typeName = typeName.toLowerCase();
        // check if type is varchar(xx)
        Pattern vcpattern = Pattern.compile("varchar\\(\\s*(\\d+)\\s*\\)");
        Matcher vcmatcher = vcpattern.matcher(typeName);
        if (vcmatcher.find()) {
            String vlen = vcmatcher.group(1);
            if (!vlen.isEmpty()) {
                return VarcharType.createVarcharType(Integer.parseInt(vlen));
            }
            return UnknownType.UNKNOWN;
        }
        // check if type is char(xx)
        Pattern cpattern = Pattern.compile("char\\(\\s*(\\d+)\\s*\\)");
        Matcher cmatcher = cpattern.matcher(typeName);
        if (cmatcher.find()) {
            String clen = cmatcher.group(1);
            if (!clen.isEmpty()) {
                return CharType.createCharType(Integer.parseInt(clen));
            }
            return UnknownType.UNKNOWN;
        }
        // check if type is decimal(precision, scale)
        Pattern dpattern = Pattern.compile("decimal\\((\\d+)\\s*,?\\s*(\\d*)\\)");
        Matcher dmatcher = dpattern.matcher(typeName);
        if (dmatcher.find()) {
            String dprecision = dmatcher.group(1);
            String dscale = dmatcher.group(2);
            if (dprecision.isEmpty()) {
                return UnknownType.UNKNOWN;
            }
            if (dscale.isEmpty()) {
                return DecimalType.createDecimalType(Integer.parseInt(dprecision));
            }
            return DecimalType.createDecimalType(Integer.parseInt(dprecision), Integer.parseInt(dscale));
        }
        switch (typeName) {
            case "boolean": return BooleanType.BOOLEAN;
            case "tinyint": return TinyintType.TINYINT;
            case "smallint": return SmallintType.SMALLINT;
            case "integer": return IntegerType.INTEGER;
            case "bigint": return BigintType.BIGINT;
            case "real": return RealType.REAL;
            case "double": return DoubleType.DOUBLE;
            case "date": return DateType.DATE;
            case "time": return TimeType.TIME;
            case "timestamp": return TimestampType.TIMESTAMP;
            default: return UnknownType.UNKNOWN;
        }
    }

    private Function parseFunction(String function)
    {
        switch (function) {
            case "function0": return new Function0(80);
        }
        return null;
    }

    // form concatenated name from database and table
    private String formName(String database, String table)
    {
        return database + "." + table;
    }

    private Path formPath(String dirOrFile1, String dirOrFile2)
    {
        String base = config.getMetaserverStore();
        String path1 = dirOrFile1;
        String path2 = dirOrFile2;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path1.startsWith("/")) {
            path1 = "/" + path1;
        }
        if (path1.endsWith("/")) {
            path1 = path1.substring(0, path1.length() - 2);
        }
        if (!path2.startsWith("/")) {
            path2 = "/" + path2;
        }
        return Path.mergePaths(Path.mergePaths(new Path(base), new Path(path1)), new Path(path2));
    }

    private Path formPath(String dirOrFile)
    {
        String base = config.getMetaserverStore();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!dirOrFile.startsWith("/")) {
            dirOrFile = "/" + dirOrFile;
        }
        return Path.mergePaths(new Path(base), new Path(dirOrFile));
    }
}
