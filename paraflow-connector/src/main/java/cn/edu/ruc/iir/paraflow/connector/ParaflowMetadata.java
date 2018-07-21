package cn.edu.ruc.iir.paraflow.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static cn.edu.ruc.iir.paraflow.connector.Types.checkType;
import static java.util.Objects.requireNonNull;

public class ParaflowMetadata
implements ConnectorMetadata
{
    private final String connectorId;
    private final ParaflowMetadataClient metadataClient;

    @Inject
    public ParaflowMetadata(ParaflowMetadataClient metadataClient, ParaflowConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId.toString(), "connectorId is null");
        this.metadataClient = requireNonNull(metadataClient, "paraflowMetadataClient is null");
    }

    /**
     * Returns the schemas provided by this connector.
     *
     * @param session session
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadataClient.getAllDatabases();
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     *
     * @param session session
     * @param tableName table name
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Optional<ParaflowTableHandle> table = metadataClient.getTableHandle(connectorId,
                tableName.getSchemaName(), tableName.getTableName());
        return table.orElse(null);
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the
     * constraint summary that isn't guaranteed by the layout.
     *
     * @param session session
     * @param table table
     * @param constraint constraint
     * @param desiredColumns desired columns
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        // get table name from ConnectorTableHandle
        ParaflowTableHandle paraflowTable = checkType(table, ParaflowTableHandle.class, "table");
        // create ParaflowTableLayoutHandle
        ParaflowTableLayoutHandle tableLayout = new ParaflowTableLayoutHandle(paraflowTable);
        tableLayout.setPredicates(constraint.getSummary() != null ? Optional.of(constraint.getSummary()) : Optional.empty());
        // ConnectorTableLayout layout = new ConnectorTableLayout(ParaflowTableLayoutHandle)
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ParaflowTableLayoutHandle layoutHandle = checkType(handle, ParaflowTableLayoutHandle.class, "tableLayoutHandle");
        return new ConnectorTableLayout(layoutHandle);
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @param session session
     * @param table table
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ParaflowTableHandle paraflowTable = checkType(table, ParaflowTableHandle.class, "table");
        SchemaTableName tableName = paraflowTable.getSchemaTableName();
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = metadataClient.getTableColMetadata(connectorId, tableName.getSchemaName(),
                tableName.getTableName()).orElse(new ArrayList<>());
        return new ConnectorTableMetadata(tableName, columns);
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @param session session
     * @param schemaNameOrNull schema name
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return new ArrayList<>();
        }
        return metadataClient.listTables(new SchemaTablePrefix(schemaNameOrNull));
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @param session session
     * @param tableHandle table handle
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ParaflowTableHandle table = checkType(tableHandle, ParaflowTableHandle.class, "table");
        List<ParaflowColumnHandle> cols = metadataClient.getTableColumnHandle(connectorId, table.getSchemaName(), table.getTableName())
                .orElse(new ArrayList<>());
        Map<String, ColumnHandle> columnMap = new HashMap<>();
        for (ParaflowColumnHandle col : cols) {
            columnMap.putIfAbsent(col.getName(), col);
        }
        return columnMap;
    }

    /**
     * Gets the metadata for the specified table column.
     *
     * @param session session
     * @param tableHandle table handle
     * @param columnHandle column handle
     * @throws RuntimeException if table or column handles are no longer valid
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ParaflowColumnHandle column = (ParaflowColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), column.getType(), column.getComment(), false);
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     *
     * @param session session
     * @param prefix prefix
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        List<SchemaTableName> tableNames = metadataClient.listTables(prefix);
        for (SchemaTableName table : tableNames) {
            List<ColumnMetadata> columnMetadatas = metadataClient.getTableColMetadata(connectorId, table.getSchemaName(),
                    table.getTableName()).orElse(new ArrayList<>());
            tableColumns.putIfAbsent(table, columnMetadatas);
        }
        return tableColumns;
    }

    /**
     * Creates a schema.
     *
     * @param session session
     * @param schemaName schema name
     * @param properties: contains comment, location and owner
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        ParaflowDatabase database = new ParaflowDatabase(schemaName);
        metadataClient.createDatabase(session, database);
    }
}
