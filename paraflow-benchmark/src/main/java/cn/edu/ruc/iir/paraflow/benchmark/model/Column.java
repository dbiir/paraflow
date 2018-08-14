package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

/**
 * paraflow
 *
 * @author guodong
 */
public interface Column<E extends Model>
{
    String getColumnName();

    TpchColumnType getType();

    double getDouble(E entity);

    long getIdentifier(E entity);

    int getInteger(E entity);

    String getString(E entity);

    int getDate(E entity);

    String TPCH_COLUMN_VALID_PREFIX_REGEX = "(?i)^(lo|c|n|r)_";

    default String getSimplifiedColumnName()
    {
        return getColumnName().replaceFirst(TPCH_COLUMN_VALID_PREFIX_REGEX, "");
    }
}
