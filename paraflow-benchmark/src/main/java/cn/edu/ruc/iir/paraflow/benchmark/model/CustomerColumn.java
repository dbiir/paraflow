package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.TpchColumnTypes.DOUBLE;
import static io.airlift.tpch.TpchColumnTypes.IDENTIFIER;
import static io.airlift.tpch.TpchColumnTypes.varchar;

/**
 * paraflow
 *
 * @author guodong
 */
public enum CustomerColumn
        implements Column<Customer>
{
    CUSTOMER_KEY("c_custkey", IDENTIFIER),
    NAME("c_name", varchar(25)),
    ADDRESS("c_address", varchar(40)),
    NATION_KEY("c_nationkey", IDENTIFIER),
    PHONE("c_phone", varchar(15)),
    ACCOUNT_BALANCE("c_acctbal", DOUBLE),
    MARKET_SEGMENT("c_mktsegment", varchar(10)),
    COMMENT("c_comment", varchar(117));

    private final String columnName;
    private final TpchColumnType type;

    CustomerColumn(String columnName, TpchColumnType type)
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
