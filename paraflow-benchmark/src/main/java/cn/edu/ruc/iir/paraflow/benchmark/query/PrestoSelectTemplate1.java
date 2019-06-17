package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow
 * 1. filter with timestamp
 *
 * @author guodong
 */
public class PrestoSelectTemplate1
        extends QueryTemplate
{
    private final int[] selection = {1, 2, 4, 6, 8, 10};
    private final long maxTime;
    private final long minTime;
    private final long selectionSlice;
    private final Random random;
    private int selectIndex = 0;

    public PrestoSelectTemplate1(QueryDistribution distribution, String table)
    {
        super(table);
        this.maxTime = distribution.getValue("max-time");
        this.minTime = distribution.getValue("min-time");
        this.selectionSlice = (maxTime - minTime) / 100;
        this.random = new Random(2344449595L);
    }

    @Override
    String makeQuery()
    {
        long timePoint = random.nextInt((int) (maxTime - minTime));
        long timeScale = selection[selectIndex++ % selection.length] * selectionSlice;
        return "SELECT SUM(lo_quantity) AS sum_qty , AVG(lo_extendedprice) AS avg_price, avg(lo_discount) AS avg_disc, count(*) AS rs_num, min(lo_lineorderkey) AS min_lineorderkey, max(lo_lineorderkey) AS max_lineorderkey FROM "
                + table
                + " WHERE lo_creation>" + (timePoint + minTime)
                + " AND lo_creation<" + (timePoint + minTime + timeScale);
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.SELECT;
    }
}
