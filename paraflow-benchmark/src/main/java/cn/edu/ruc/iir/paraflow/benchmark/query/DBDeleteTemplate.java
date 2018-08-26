package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public class DBDeleteTemplate
        extends QueryTemplate
{
    private static final int BASE = 10_000_000;
    private int counter = 0;

    DBDeleteTemplate(String table)
    {
        super(table);
    }

    @Override
    String makeQuery()
    {
        counter++;
        return "DELETE FROM " + table + " WHERE c_custkey=" + (BASE + counter) + ";";
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.DELETE;
    }
}
