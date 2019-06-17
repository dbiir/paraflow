package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class QueryGenerator
{
    protected QueryDistribution distribution;

    public QueryGenerator(QueryDistribution distribution)
    {
        this.distribution = distribution;
    }

    public abstract boolean hasNext();

    public abstract String next();

    public enum QueryType
    {
        UPDATE, INSERT, DELETE, SELECT
    }
}
