package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.generator.CustomerGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.generator.LineOrderGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.generator.NationGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.generator.RegionGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.model.Column;
import cn.edu.ruc.iir.paraflow.benchmark.model.Customer;
import cn.edu.ruc.iir.paraflow.benchmark.model.CustomerColumn;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn;
import cn.edu.ruc.iir.paraflow.benchmark.model.Model;
import cn.edu.ruc.iir.paraflow.benchmark.model.Nation;
import cn.edu.ruc.iir.paraflow.benchmark.model.NationColumn;
import cn.edu.ruc.iir.paraflow.benchmark.model.Region;
import cn.edu.ruc.iir.paraflow.benchmark.model.RegionColumn;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public abstract class TpchTable<E extends Model>
{
    public static final TpchTable<Customer> CUSTOMER = new TpchTable<Customer>("customer", CustomerColumn.values())
    {
        public Iterable<Customer> createGenerator(double scaleFactor, int part, int partCount, long minCustomerKey, long numCustomerKey)
        {
            return new CustomerGenerator(scaleFactor, part, partCount);
        }
    };

    public static final TpchTable<LineOrder> LINEORDER = new TpchTable<LineOrder>("lineorder", LineOrderColumn.values())
    {
        public Iterable<LineOrder> createGenerator(double scaleFactor, int part, int partCount, long minCustomerKey, long numCustomerKey)
        {
            return new LineOrderGenerator(scaleFactor, part, partCount, minCustomerKey, numCustomerKey);
        }
    };

    public static final TpchTable<Nation> NATION = new TpchTable<Nation>("nation", NationColumn.values())
    {
        public Iterable<Nation> createGenerator(double scaleFactor, int part, int partCount, long minCustomerKey, long numCustomerKey)
        {
            if (part != 1) {
                return ImmutableList.of();
            }
            return new NationGenerator();
        }
    };

    public static final TpchTable<Region> REGION = new TpchTable<Region>("region", RegionColumn.values())
    {
        public Iterable<Region> createGenerator(double scaleFactor, int part, int partCount, long minCustomerKey, long numCustomerKey)
        {
            if (part != 1) {
                return ImmutableList.of();
            }
            return new RegionGenerator();
        }
    };

    private static final List<TpchTable<?>> TABLES;
    private static final Map<String, TpchTable<?>> TABLES_BY_NAME;

    static {
        TABLES = ImmutableList.of(CUSTOMER, LINEORDER, NATION, REGION);
        TABLES_BY_NAME = Maps.uniqueIndex(TABLES, TpchTable::getTableName);
    }

    private final String tableName;
    private final List<Column<E>> columns;
    private final Map<String, Column<E>> columnsByName;

    private TpchTable(String tableName, Column<E>[] columns)
    {
        this.tableName = tableName;
        this.columns = ImmutableList.copyOf(columns);
        this.columnsByName = new ImmutableMap.Builder<String, Column<E>>()
                .putAll(Maps.uniqueIndex(this.columns, Column::getColumnName))
                .putAll(Maps.uniqueIndex(this.columns, Column::getSimplifiedColumnName))
                .build();
    }

    public static List<TpchTable<?>> getTables()
    {
        return TABLES;
    }

    public static TpchTable<?> getTable(String tableName)
    {
        TpchTable<?> table = TABLES_BY_NAME.get(tableName);
        Preconditions.checkArgument(table != null, "Table %s not found", tableName);
        return table;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<Column<E>> getColumns()
    {
        return columns;
    }

    public Column<E> getColumn(String columnName)
    {
        Column<E> column = columnsByName.get(columnName);
        Preconditions.checkArgument(column != null, "Table %s does not have a column %s", tableName, columnName);
        return column;
    }

    public abstract Iterable<E> createGenerator(double scaleFactor, int part, int partCount, long minCustomerKey, long numCustomerKey);
}
