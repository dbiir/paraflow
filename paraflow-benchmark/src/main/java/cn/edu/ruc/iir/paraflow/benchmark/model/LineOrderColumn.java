package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.TpchColumnTypes.DATE;
import static io.airlift.tpch.TpchColumnTypes.DOUBLE;
import static io.airlift.tpch.TpchColumnTypes.IDENTIFIER;
import static io.airlift.tpch.TpchColumnTypes.INTEGER;
import static io.airlift.tpch.TpchColumnTypes.varchar;

/**
 * paraflow
 *
 * @author guodong
 */
public enum LineOrderColumn
        implements Column<LineOrder>
{
    @SuppressWarnings("SpellCheckingInspection")
    CUSTOMER_KEY("lo_custkey", IDENTIFIER),
    @SuppressWarnings("SpellCheckingInspection")
    LINEORDER_KEY("lo_lineorderkey", IDENTIFIER),
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_STATUS("lo_orderstatus", varchar(1)),
    @SuppressWarnings("SpellCheckingInspection")
    TOTAL_PRICE("lo_totalprice", DOUBLE),
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_DATE("lo_orderdate", DATE),
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_PRIORITY("lo_orderpriority", varchar(15)),
    CLERK("lo_clerk", varchar(15)),
    @SuppressWarnings("SpellCheckingInspection")
    SHIP_PRIORITY("lo_shippriority", INTEGER),
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_COMMENT("lo_ordercomment", varchar(79)),
    @SuppressWarnings("SpellCheckingInspection")
    LINE_NUMBER("lo_linenumber", INTEGER),
    @SuppressWarnings("SpellCheckingInspection")
    QUANTITY("lo_quantity", DOUBLE),
    @SuppressWarnings("SpellCheckingInspection")
    EXTENDED_PRICE("lo_extendedprice", DOUBLE),
    DISCOUNT("lo_discount", DOUBLE),
    TAX("lo_tax", DOUBLE),
    @SuppressWarnings("SpellCheckingInspection")
    RETURN_FLAG("lo_returnflag", varchar(1)),
    STATUS("lo_linestatus", varchar(1)),
    @SuppressWarnings("SpellCheckingInspection")
    SHIP_DATE("lo_shipdate", DATE),
    @SuppressWarnings("SpellCheckingInspection")
    COMMIT_DATE("lo_commitdate", DATE),
    @SuppressWarnings("SpellCheckingInspection")
    RECEIPT_DATE("lo_receiptdate", DATE),
    @SuppressWarnings("SpellCheckingInspection")
    SHIP_INSTRUCTIONS("lo_shipinstruct", varchar(25)),
    @SuppressWarnings("SpellCheckingInspection")
    SHIP_MODE("lo_shipmode", varchar(10)),
    @SuppressWarnings("SpellCheckingInspection")
    LINEITEM_COMMENT("lo_lineitemcomment", varchar(44)),
    CREATION("lo_creation", INTEGER);

    private final String columnName;
    private final TpchColumnType type;

    LineOrderColumn(String columnName, TpchColumnType type)
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
