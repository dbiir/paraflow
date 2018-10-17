package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class QueryTemplate
{
    protected final String table;

    QueryTemplate(String table)
    {
        this.table = table;
    }

    abstract String makeQuery();

    abstract QueryGenerator.QueryType getType();
}
