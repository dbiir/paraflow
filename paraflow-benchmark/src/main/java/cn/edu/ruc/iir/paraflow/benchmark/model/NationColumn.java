package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.TpchColumnTypes.IDENTIFIER;
import static io.airlift.tpch.TpchColumnTypes.varchar;

/**
 * paraflow
 *
 * @author guodong
 */
public enum NationColumn
        implements Column<Nation>
{
    @SuppressWarnings("SpellCheckingInspection")
    NATION_KEY("n_nationkey", IDENTIFIER),
    NAME("n_name", varchar(25)),
    @SuppressWarnings("SpellCheckingInspection")
    REGION_KEY("n_regionkey", IDENTIFIER),
    COMMENT("n_comment", varchar(152));

    private final String columnName;
    private final TpchColumnType type;

    NationColumn(String columnName, TpchColumnType type)
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
