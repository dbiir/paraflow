package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public class DBSelectTemplate
        extends QueryTemplate
{
    DBSelectTemplate(String table)
    {
        super(table);
    }

    @Override
    String makeQuery()
    {
        return null;
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.SELECT;
    }
}
