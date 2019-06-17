package cn.edu.ruc.iir.paraflow.connector.impl;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.connector.StorageFormat;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowColumnHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowDatabase;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableLayoutHandle;
import cn.edu.ruc.iir.paraflow.connector.type.UnknownType;
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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.edu.ruc.iir.paraflow.commons.proto.StatusProto.ResponseStatus.State.STATUS_OK;
import static java.util.Objects.requireNonNull;

public class ParaflowMetaDataReader
{
    private static final Logger log = Logger.get(ParaflowMetaDataReader.class);
    private final MetaClient metaClient;
    private final ParaflowPrestoConfig config;

    // read config. check if meta table already exists in database, or else initialize tables.
    @Inject
    public ParaflowMetaDataReader(ParaflowPrestoConfig config)
    {
        this.config = config;
        this.metaClient = new MetaClient(config.getMetaserverHost(), config.getMetaserverPort());
    }

    public List<String> getAllDatabases()
    {
        MetaProto.StringListType stringList = metaClient.listDatabases();
        List<String> resultL = new ArrayList<>();
        int count = stringList.getStrCount();
        for (int i = 0; i < count; i++) {
            resultL.add(stringList.getStr(i));
        }
        return resultL;
    }

    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        String dbPrefix = prefix.getSchemaName();
        String tblPrefix = prefix.getTableName();

        // if dbPrefix not mean to match all
        String tblName;
        String dbName;
        if (dbPrefix != null && tblPrefix != null) {
            MetaProto.StringListType stringListType = metaClient.listTables(dbPrefix);
            if (stringListType.getStrCount() == 0) {
                return tables;
            }
            for (int i = 0; i < stringListType.getStrCount(); i++) {
                tblName = stringListType.getStr(0);
                dbName = dbPrefix;
                tables.add(new SchemaTableName(dbName, tblName));
            }
        }
        return tables;
    }

    public List<SchemaTableName> listTables(String dbPrefix)
    {
        List<SchemaTableName> tables = new ArrayList<>();

        // if dbPrefix not mean to match all
        String tblName;
        String dbName;
        if (dbPrefix != null) {
            MetaProto.StringListType stringListType = metaClient.listTables(dbPrefix);
            if (stringListType.getStrCount() == 0) {
                return tables;
            }
            for (int i = 0; i < stringListType.getStrCount(); i++) {
                tblName = stringListType.getStr(0);
                dbName = dbPrefix;
                tables.add(new SchemaTableName(dbName, tblName));
            }
        }
        return tables;
    }

    public List<String> getTableNames(String dbPrefix)
    {
        List<String> tables = new ArrayList<>();

        // if dbPrefix not mean to match all
        String tblName;
        if (dbPrefix != null) {
            MetaProto.StringListType stringListType = metaClient.listTables(dbPrefix);
            if (stringListType.getStrCount() == 0) {
                return tables;
            }
            for (int i = 0; i < stringListType.getStrCount(); i++) {
                tblName = stringListType.getStr(i);
                tables.add(tblName);
            }
        }
        return tables;
    }

    public Optional<ParaflowTableHandle> getTableHandle(String connectorId, String dbName, String tblName)
    {
        ParaflowTableHandle table;
        MetaProto.TblParam tblParam = metaClient.getTable(dbName, tblName);
        if (tblParam.getIsEmpty()) {
            return Optional.empty();
        }
        String location = tblParam.getLocationUrl();
        table = new ParaflowTableHandle(
                requireNonNull(connectorId, "connectorId is null"),
                requireNonNull(dbName, "database name is null"),
                requireNonNull(tblName, "table name is null"),
                requireNonNull(location, "location uri is null"));
        return Optional.of(table);
    }

    public Optional<ParaflowTableLayoutHandle> getTableLayout(String connectorId, String dbName, String tblName)
    {
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
        int fiberColId = tblParam.getFiberColId();
        int timeColId = tblParam.getTimeColId();
        String fiberColName = metaClient.getColumnName(tblParam.getDbId(), tblParam.getTblId(), fiberColId).getColumn();
        String timeColName = metaClient.getColumnName(tblParam.getDbId(), tblParam.getTblId(), timeColId).getColumn();
        String partitionerName = tblParam.getFuncName();
//        ParaflowFiberPartitioner partitioner = parsePartitioner(partitionerName);
//        if (partitioner == null) {
//            log.error("partitioner parse error");
//            return Optional.empty();
//        }

        if (fiberColName.equals("") || timeColName.equals("")) {
            tableLayout = new ParaflowTableLayoutHandle(tableHandle);
        }
        else {
            // construct ColumnHandle
            ParaflowColumnHandle fiberCol = getColumnHandle(connectorId, fiberColName, tblName, dbName);
            ParaflowColumnHandle timeCol = getColumnHandle(connectorId, timeColName, tblName, dbName);
            tableLayout = new ParaflowTableLayoutHandle(tableHandle, fiberCol, timeCol, partitionerName, StorageFormat.PARQUET, Optional.empty());
        }
        return Optional.of(tableLayout);
    }

    /**
     * Get all column handles of specified table
     */
    public Optional<List<ParaflowColumnHandle>> getTableColumnHandle(String connectorId, String dbName, String tblName)
    {
        log.debug("Get list of column handles of table " + formName(dbName, tblName));
        List<ParaflowColumnHandle> columnHandles = new ArrayList<>();
        String colName;
        int colTypeName;
        String dataTypeName;
        MetaProto.StringListType listColumns = metaClient.listColumns(dbName, tblName);
        if (listColumns.getIsEmpty()) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        for (int i = 0; i < listColumns.getStrCount(); i++) {
            colName = listColumns.getStr(i);
            MetaProto.ColParam colParam = metaClient.getColumn(dbName, tblName, colName);
            colTypeName = colParam.getColType();
            dataTypeName = colParam.getDataType();
            // Deal with col type
            ParaflowColumnHandle.ColumnType colType = getColType(colTypeName);
            // Deal with data type
            Type type = getType(dataTypeName);
            columnHandles.add(new ParaflowColumnHandle(colName, type, "", colType, connectorId));
        }
        return Optional.of(columnHandles);
    }

    public void shutdown()
    {
        metaClient.shutdownNow();
    }

    private ParaflowColumnHandle getColumnHandle(String connectorId, String colName, String tblName, String dbName)
    {
        MetaProto.ColParam colParam = metaClient.getColumn(dbName, tblName, colName);
        if (colParam.getIsEmpty()) {
            log.error("Match more/less than one column");
        }
        int colTypeId = colParam.getColType();
        String dataType = colParam.getDataType();
        // Deal with colType
        ParaflowColumnHandle.ColumnType colType = getColType(colTypeId);
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
        StatusProto.ResponseStatus responseStatus
                = metaClient.createDatabase(dbName, dbPath, " ");
        if (responseStatus.getStatus() == STATUS_OK) {
            log.debug("Create database " + dbName + " succeed!");
        }
        else {
            log.debug("Create database " + dbName + " failed!");
        }
    }

    public void dropDatabase(String dbName)
    {
        metaClient.deleteDatabase(dbName);
    }

    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        List<String> columnName = new LinkedList<>();
        List<String> dataType = new LinkedList<>();
        for (ColumnMetadata column : columns) {
            columnName.add(column.getName());
            dataType.add(column.getType().getDisplayName());
        }

        String tblName = tableMetadata.getTable().getTableName();
        String dbName = tableMetadata.getTable().getSchemaName();
        // createTable
        metaClient.createTable(dbName, tblName, session.getUser(),
                "", -1, "",
                -1, columnName, dataType);
    }

    public void dropTable(String schemaName, String tableName)
    {
        metaClient.deleteTable(schemaName, tableName);
    }

    public List<String> filterBlocks(String db, String table, int fiberId, long timeLow, long timeHigh)
    {
        MetaProto.StringListType stringListType;
        if (fiberId == -1) {
            stringListType = metaClient.filterBlockIndex(db, table, timeLow, timeHigh);
        }
        else {
            stringListType = metaClient.filterBlockIndexByFiber(db, table, fiberId, timeLow, timeHigh);
        }
        List<String> resultL = new ArrayList<>();
        for (int i = 0; i < stringListType.getStrCount(); i++) {
            resultL.add(stringListType.getStr(i));
        }
        return resultL;
    }

    private ParaflowColumnHandle.ColumnType getColType(int typeName)
    {
        log.debug("Get col type " + typeName);
        switch (typeName) {
            case 1:
                return ParaflowColumnHandle.ColumnType.FIBER;
            case 2:
                return ParaflowColumnHandle.ColumnType.TIMESTAMP;
            case 0:
                return ParaflowColumnHandle.ColumnType.REGULAR;
            default:
                log.error("No match col type!");
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
            case "boolean":
                return BooleanType.BOOLEAN;
            case "tinyint":
                return TinyintType.TINYINT;
            case "smallint":
                return SmallintType.SMALLINT;
            case "integer":
                return IntegerType.INTEGER;
            case "bigint":
                return BigintType.BIGINT;
            case "real":
                return RealType.REAL;
            case "double":
                return DoubleType.DOUBLE;
            case "date":
                return DateType.DATE;
            case "time":
                return TimeType.TIME;
            case "timestamp":
                return TimestampType.TIMESTAMP;
            default:
                return UnknownType.UNKNOWN;
        }
    }

    private Path formPath(String dirOrFile)
    {
        String base = this.config.getHDFSWarehouse();
        String path = dirOrFile;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return Path.mergePaths(new Path(base), new Path(path));
    }

    // form concatenated name from database and table
    private String formName(String database, String table)
    {
        return database + "." + table;
    }
}
