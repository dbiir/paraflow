package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public class DBUpdateTemplate
        extends QueryTemplate
{
    @Override
    String makeQuery()
    {
        return "UPDATE TABLE SET c_name='' WHERE c_custkey= 1";
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.UPDATE;
    }
}
