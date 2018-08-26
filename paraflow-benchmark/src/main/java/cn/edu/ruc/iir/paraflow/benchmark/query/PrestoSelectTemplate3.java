package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow
 * 3. join with customer
 *
 * @author guodong
 */
public class PrestoSelectTemplate3
        extends QueryTemplate
{
    private final long maxCustkey;
    private final Random random;

    public PrestoSelectTemplate3(QueryDistribution distribution)
    {
        this.maxCustkey = distribution.getValue("max-custkey");
        this.random = new Random(8833948812L);
    }

    @Override
    String makeQuery()
    {
        int custkey = random.nextInt((int) maxCustkey);
        return "SELECT c_name, c_address, c_phone, SUM(lo_quantity) AS sum_qty , AVG(lo_extendedprice) AS avg_price, avg(lo_discount) AS avg_disc, count(*) AS rs_num, min(lo_lineorderkey) AS min_lineorderkey, max(lo_orderkey) AS max_lineorderkey FROM lineorder, customer WHERE lineorder.lo_custkey=customer.c_custkey AND lo_custkey="
                + custkey;
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.SELECT;
    }
}
