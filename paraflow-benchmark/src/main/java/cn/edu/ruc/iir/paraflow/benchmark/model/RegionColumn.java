package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.TpchColumnTypes.IDENTIFIER;
import static io.airlift.tpch.TpchColumnTypes.varchar;

/**
 * paraflow
 *
 * @author guodong
 */
public enum RegionColumn
        implements Column<Region>
{
    @SuppressWarnings("SpellCheckingInspection")
    REGION_KEY("r_regionkey", IDENTIFIER),
    NAME("r_name", varchar(25)),
    COMMENT("r_comment", varchar(152));

    private final String columnName;
    private final TpchColumnType type;

    RegionColumn(String columnName, TpchColumnType type)
    {
        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public TpchColumnType getType()
    {
        return type;
    }
}
