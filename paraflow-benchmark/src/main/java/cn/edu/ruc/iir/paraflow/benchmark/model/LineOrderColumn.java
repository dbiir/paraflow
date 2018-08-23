package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

import static io.airlift.tpch.GenerateUtils.formatDate;
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
    CUSTOMER_KEY("lo_custkey", IDENTIFIER)
            {
                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getCustomerKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINEORDER_KEY("lo_lineorderkey", IDENTIFIER)
            {
                @Override
                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getLineOrderKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_STATUS("lo_orderstatus", varchar(1))
            {
                public String getString(LineOrder lineorder)
                {
                    return String.valueOf(lineorder.getOrderStatus());
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    TOTAL_PRICE("lo_totalprice", DOUBLE)
            {
                public double getDouble(LineOrder lineorder)
                {
                    return lineorder.getTotalPrice();
                }

                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getTotalPriceInCents();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_DATE("lo_orderdate", DATE)
            {
                @Override
                public String getString(LineOrder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(LineOrder lineorder)
                {
                    return lineorder.getOrderDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_PRIORITY("lo_orderpriority", varchar(15))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getOrderPriority();
                }
            },

    CLERK("lo_clerk", varchar(15))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getClerk();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_PRIORITY("lo_shippriority", INTEGER)
            {
                public int getInteger(LineOrder lineorder)
                {
                    return lineorder.getShipPriority();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_COMMENT("lo_ordercomment", varchar(79))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getOrderComment();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINE_NUMBER("lo_linenumber", INTEGER)
            {
                public int getInteger(LineOrder lineorder)
                {
                    return lineorder.getLineNumber();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    QUANTITY("lo_quantity", DOUBLE)
            {
                public double getDouble(LineOrder lineorder)
                {
                    return lineorder.getQuantity();
                }

                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getQuantity() * 100;
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    EXTENDED_PRICE("lo_extendedprice", DOUBLE)
            {
                public double getDouble(LineOrder lineorder)
                {
                    return lineorder.getExtendedPrice();
                }

                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getExtendedPriceInCents();
                }
            },

    DISCOUNT("lo_discount", DOUBLE)
            {
                public double getDouble(LineOrder lineorder)
                {
                    return lineorder.getDiscount();
                }

                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getDiscountPercent();
                }
            },

    TAX("lo_tax", DOUBLE)
            {
                public double getDouble(LineOrder lineorder)
                {
                    return lineorder.getTax();
                }

                public long getIdentifier(LineOrder lineorder)
                {
                    return lineorder.getTaxPercent();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RETURN_FLAG("lo_returnflag", varchar(1))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getReturnFlag();
                }
            },

    STATUS("lo_linestatus", varchar(1))
            {
                public String getString(LineOrder lineorder)
                {
                    return String.valueOf(lineorder.getLineStatus());
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_DATE("lo_shipdate", DATE)
            {
                public String getString(LineOrder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(LineOrder lineorder)
                {
                    return lineorder.getShipDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    COMMIT_DATE("lo_commitdate", DATE)
            {
                public String getString(LineOrder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                public int getDate(LineOrder lineorder)
                {
                    return lineorder.getCommitDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RECEIPT_DATE("lo_receiptdate", DATE)
            {
                public String getString(LineOrder lineorder)
                {
                    return formatDate(getDate(lineorder));
                }

                @Override
                public int getDate(LineOrder lineorder)
                {
                    return lineorder.getReceiptDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_INSTRUCTIONS("lo_shipinstruct", varchar(25))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getShipInstructions();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_MODE("lo_shipmode", varchar(10))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getShipMode();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINEITEM_COMMENT("lo_lineitemcomment", varchar(44))
            {
                public String getString(LineOrder lineorder)
                {
                    return lineorder.getLineitemComment();
                }
            },

    CREATION("lo_creation", INTEGER)
            {
                public String getString(LineOrder lineOrder)
                {
                    return String.valueOf(lineOrder.getCreation());
                }
            };

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

    @Override
    public double getDouble(LineOrder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(LineOrder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(LineOrder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(LineOrder lineorder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(LineOrder entity)
    {
        throw new UnsupportedOperationException();
    }
}
