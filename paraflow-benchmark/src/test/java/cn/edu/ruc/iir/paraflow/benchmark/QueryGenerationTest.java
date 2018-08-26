package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.query.DBQueryGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.query.PrestoQueryGenerator;
import cn.edu.ruc.iir.paraflow.benchmark.query.QueryDistribution;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class QueryGenerationTest
{
    @Test
    public void testPrestoQueryGenerator()
    {
        QueryDistribution queryDistribution = new QueryDistribution();
        queryDistribution.setDistribution("t1", 1);
        queryDistribution.setDistribution("t2", 1);
        queryDistribution.setDistribution("t3", 1);
        queryDistribution.setDistribution("max-custkey", 1000000);
        queryDistribution.setDistribution("min-time", 200000000L);
        queryDistribution.setDistribution("max-time", 200008000L);
        queryDistribution.setSizeLimit(100);
        PrestoQueryGenerator queryGenerator = new PrestoQueryGenerator(queryDistribution, "lineorder", "customer");
        while (queryGenerator.hasNext()) {
            System.out.println(queryGenerator.next());
        }
    }

    @Test
    public void testDBQueryGenerator()
    {
        QueryDistribution queryDistribution = new QueryDistribution();
        queryDistribution.setDistribution("select", 0);
        queryDistribution.setDistribution("insert", 1);
        queryDistribution.setDistribution("update", 9);
        queryDistribution.setDistribution("delete", 0);
        queryDistribution.setTimeLimit(1000);
        DBQueryGenerator queryGenerator = new DBQueryGenerator(queryDistribution, "customer");
        while (queryGenerator.hasNext()) {
            System.out.println(queryGenerator.next());
        }
    }
}
