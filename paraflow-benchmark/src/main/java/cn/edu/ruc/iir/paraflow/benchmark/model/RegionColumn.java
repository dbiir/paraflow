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
    REGION_KEY("r_regionkey", IDENTIFIER)
            {
                public long getIdentifier(Region region)
                {
                    return region.getRegionKey();
                }
            },

    NAME("r_name", varchar(25))
            {
                public String getString(Region region)
                {
                    return region.getName();
                }
            },

    COMMENT("r_comment", varchar(152))
            {
                public String getString(Region region)
                {
                    return region.getComment();
                }
            };

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

    @Override
    public double getDouble(Region region)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Region region)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Region region)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Region region)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Region entity)
    {
        throw new UnsupportedOperationException();
    }
}
