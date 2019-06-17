package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow
 * 2. filter with customer and timestamp
 *
 * @author guodong
 */
public class PrestoSelectTemplate2
        extends QueryTemplate
{
    private final int[] selection = {1, 2, 4, 6, 8, 10};
    private final long maxTime;
    private final long minTime;
    private final long maxCustkey;
    private final long selectionSlice;
    private final Random timePointRandom;
    private final Random custkeyRandom;
    private int selectIndex = 0;

    public PrestoSelectTemplate2(QueryDistribution distribution, String table)
    {
        super(table);
        this.maxTime = distribution.getValue("max-time");
        this.minTime = distribution.getValue("min-time");
        this.maxCustkey = distribution.getValue("max-custkey");
        this.selectionSlice = (maxTime - minTime) / 100;
        this.timePointRandom = new Random(2395883929L);
        this.custkeyRandom = new Random(2399431038L);
    }

    @Override
    String makeQuery()
    {
        long timePoint = timePointRandom.nextInt((int) (maxTime - minTime));
        long timeScale = selection[selectIndex++ % selection.length] * selectionSlice;
        long custkey = custkeyRandom.nextInt((int) maxCustkey);
        return "SELECT SUM(lo_quantity) AS sum_qty , AVG(lo_extendedprice) AS avg_price, avg(lo_discount) AS avg_disc, count(*) AS rs_num, min(lo_lineorderkey) AS min_lineorderkey, max(lo_lineorderkey) AS max_lineorderkey FROM "
                + table
                + " WHERE lo_creation>" + (timePoint + minTime)
                + " AND lo_creation<" + (timePoint + minTime + timeScale) + " AND lo_custkey=" + custkey;
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.SELECT;
    }
}
