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

    String TPCH_COLUMN_VALID_PREFIX_REGEX = "(?i)^(lo|c|n|r)_";

    default String getSimplifiedColumnName()
    {
        return getColumnName().replaceFirst(TPCH_COLUMN_VALID_PREFIX_REGEX, "");
    }
}
