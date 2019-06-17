/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowColumnHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowDatabase;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableLayoutHandle;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowMetaDataReader;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ParaflowMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final ParaflowMetaDataReader paraflowMetaDataReader;

    @Inject
    public ParaflowMetadata(ParaflowMetaDataReader paraflowMetaDataReader, ParaflowConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId.toString(), "connectorId is null");
        this.paraflowMetaDataReader = requireNonNull(paraflowMetaDataReader, "paraflowMetaDataReader is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return paraflowMetaDataReader.getAllDatabases();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ParaflowTableHandle> table = paraflowMetaDataReader.getTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
        return table.orElse(null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ParaflowTableHandle paraflowTable = checkType(table, ParaflowTableHandle.class, "table");
        SchemaTableName tableName = paraflowTable.getSchemaTableName();
        ParaflowTableLayoutHandle tableLayout = paraflowMetaDataReader.getTableLayout(connectorId, tableName.getSchemaName(), tableName.getTableName()).orElse(null);
        if (tableLayout == null) {
            return ImmutableList.of();
        }
        tableLayout.setPredicates(constraint.getSummary() != null ? Optional.of(constraint.getSummary()) : Optional.empty());
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ParaflowTableLayoutHandle layoutHandle = checkType(handle, ParaflowTableLayoutHandle.class, "tableLayoutHandle");
        return new ConnectorTableLayout(layoutHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ParaflowTableHandle paraflowTable = (ParaflowTableHandle) table;
        checkArgument(paraflowTable.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(paraflowTable.getSchemaName(), paraflowTable.getTableName());
        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        }
        else {
            schemaNames = paraflowMetaDataReader.getAllDatabases();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : paraflowMetaDataReader.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ParaflowTableHandle table = checkType(tableHandle, ParaflowTableHandle.class, "table");
        List<ParaflowColumnHandle> cols = paraflowMetaDataReader.getTableColumnHandle(connectorId, table.getSchemaName(), table.getTableName())
                .orElse(new ArrayList<>());
        Map<String, ColumnHandle> columnMap = new HashMap<>();
        for (ParaflowColumnHandle col : cols) {
            columnMap.putIfAbsent(col.getName(), col);
        }
        return columnMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ParaflowColumnHandle column = (ParaflowColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), column.getType(), column.getComment(), false);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        List<SchemaTableName> tableNames = paraflowMetaDataReader.listTables(prefix);
        for (SchemaTableName table : tableNames) {
            List<ColumnMetadata> columnMetadatas = paraflowMetaDataReader.getTableColMetadata(connectorId, table.getSchemaName(),
                    table.getTableName()).orElse(new ArrayList<>());
            tableColumns.putIfAbsent(table, columnMetadatas);
        }
        return tableColumns;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        ParaflowDatabase database = new ParaflowDatabase(schemaName);
        paraflowMetaDataReader.createDatabase(session, database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        paraflowMetaDataReader.dropDatabase(schemaName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        paraflowMetaDataReader.createTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ParaflowTableHandle paraflowTableHandle = (ParaflowTableHandle) tableHandle;
        paraflowMetaDataReader.dropTable(paraflowTableHandle.getSchemaName(), paraflowTableHandle.getTableName());
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = paraflowMetaDataReader.getTableColMetadata(connectorId, tableName.getSchemaName(),
                tableName.getTableName()).orElse(new ArrayList<>());
        return new ConnectorTableMetadata(tableName, columns);
    }
}
