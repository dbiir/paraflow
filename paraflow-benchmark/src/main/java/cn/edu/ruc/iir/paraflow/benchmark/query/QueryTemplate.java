package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class QueryTemplate
{
    abstract String makeQuery();

    abstract QueryGenerator.QueryType getType();
}
