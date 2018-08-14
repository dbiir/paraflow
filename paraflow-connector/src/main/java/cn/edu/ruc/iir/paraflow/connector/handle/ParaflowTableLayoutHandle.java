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
package cn.edu.ruc.iir.paraflow.connector.handle;

import cn.edu.ruc.iir.paraflow.connector.StorageFormat;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowTableLayoutHandle
implements ConnectorTableLayoutHandle
{
    private final ParaflowTableHandle table;
    private final ParaflowColumnHandle fiberColumn;
    private final ParaflowColumnHandle timestampColumn;
    private final String fiberPartitioner;
    private final StorageFormat storageFormat;
    private Optional<TupleDomain<ColumnHandle>> predicates;

    public ParaflowTableLayoutHandle(ParaflowTableHandle table)
    {
        this(table,
                new ParaflowColumnHandle("null", IntegerType.INTEGER, "", ParaflowColumnHandle.ColumnType.REGULAR, ""),
                new ParaflowColumnHandle("null", IntegerType.INTEGER, "", ParaflowColumnHandle.ColumnType.REGULAR, ""),
                "",
                StorageFormat.PARQUET,
                Optional.empty());
    }

    @JsonCreator
    public ParaflowTableLayoutHandle(
            @JsonProperty("table") ParaflowTableHandle table,
            @JsonProperty("fiberColumn") ParaflowColumnHandle fiberColumn,
            @JsonProperty("timestampColumn") ParaflowColumnHandle timestampColumn,
            @JsonProperty("fiberPartitioner") String fiberPartitioner,
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("predicates") Optional<TupleDomain<ColumnHandle>> predicates)
    {
        this.table = requireNonNull(table, "table is null");
        this.fiberColumn = requireNonNull(fiberColumn, "fiberColumn is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.fiberPartitioner = requireNonNull(fiberPartitioner, "fiberPartitioner is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    @JsonProperty
    public ParaflowTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(table.getSchemaName(), table.getTableName());
    }

    @JsonProperty
    public ParaflowColumnHandle getFiberColumn()
    {
        return fiberColumn;
    }

    @JsonProperty
    public ParaflowColumnHandle getTimestampColumn()
    {
        return timestampColumn;
    }

    @JsonProperty
    public String getFiberPartitioner()
    {
        return fiberPartitioner;
    }

    @JsonProperty
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    public void setPredicates(Optional<TupleDomain<ColumnHandle>> predicates)
    {
        this.predicates = predicates;
    }

    @JsonProperty
    public Optional<TupleDomain<ColumnHandle>> getPredicates()
    {
        return predicates;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, fiberColumn, timestampColumn, fiberPartitioner, storageFormat);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ParaflowTableLayoutHandle other = (ParaflowTableLayoutHandle) obj;
        return Objects.equals(table, other.table) &&
                Objects.equals(fiberColumn, other.fiberColumn) &&
                Objects.equals(timestampColumn, other.timestampColumn) &&
                Objects.equals(fiberPartitioner, other.fiberPartitioner) &&
                Objects.equals(storageFormat, other.storageFormat);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("fiber column", fiberColumn)
                .add("timestamp column", timestampColumn)
                .add("fiber partitioner", fiberPartitioner)
                .add("storage format", storageFormat)
                .toString();
    }
}
