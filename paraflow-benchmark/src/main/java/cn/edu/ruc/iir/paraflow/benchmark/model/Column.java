package cn.edu.ruc.iir.paraflow.benchmark.model;

import io.airlift.tpch.TpchColumnType;

/**
 * paraflow
 *
 * @author guodong
 */
public interface Column<E extends Model>
{
    String TPCH_COLUMN_VALID_PREFIX_REGEX = "(?i)^(lo|c|n|r)_";

    String getColumnName();

    TpchColumnType getType();

    default String getSimplifiedColumnName()
    {
        return getColumnName().replaceFirst(TPCH_COLUMN_VALID_PREFIX_REGEX, "");
    }
}
