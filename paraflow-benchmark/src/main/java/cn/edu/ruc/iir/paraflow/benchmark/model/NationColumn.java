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
    NATION_KEY("n_nationkey", IDENTIFIER)
    {
        public long getIdentifier(Nation nation)
        {
            return nation.getNationKey();
        }
    },

    NAME("n_name", varchar(25))
    {
        public String getString(Nation nation)
        {
            return nation.getName();
        }
    },

    @SuppressWarnings("SpellCheckingInspection")
    REGION_KEY("n_regionkey", IDENTIFIER)
    {
        public long getIdentifier(Nation nation)
        {
            return nation.getRegionKey();
        }
    },

    COMMENT("n_comment", varchar(152))
    {
        public String getString(Nation nation)
        {
            return nation.getComment();
        }
    };

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

    @Override
    public double getDouble(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Nation entity)
    {
        throw new UnsupportedOperationException();
    }
}
