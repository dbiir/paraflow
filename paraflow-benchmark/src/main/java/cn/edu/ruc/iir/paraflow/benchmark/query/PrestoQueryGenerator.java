package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow
 *
 * @author guodong
 */
public class PrestoQueryGenerator
        extends QueryGenerator
{
    private final PrestoSelectTemplate1 template1;
    private final PrestoSelectTemplate2 template2;
    private final PrestoSelectTemplate3 template3;
    private final Random random;
    private final long range;
    private final long t1Floor;
    private final long t2Floor;
    private final long t3Floor;
    private long counter = 0;

    public PrestoQueryGenerator(QueryDistribution distribution, String table, String joinTable)
    {
        super(distribution);
        this.template1 = new PrestoSelectTemplate1(distribution, table);
        this.template2 = new PrestoSelectTemplate2(distribution, table);
        this.template3 = new PrestoSelectTemplate3(distribution, table, joinTable);
        this.random = new Random();
        t1Floor = 0;
        t2Floor = distribution.getValue("t1") + t1Floor;
        t3Floor = distribution.getValue("t2") + t2Floor;
        range = distribution.getValue("t3") + t3Floor;
    }

    @Override
    public boolean hasNext()
    {
        return counter < distribution.sizeLimit();
    }

    @Override
    public String next()
    {
        int randomV = random.nextInt((int) range);
        counter++;
        if (randomV >= t3Floor) {
            return template3.makeQuery();
        }
        if (randomV >= t2Floor) {
            return template2.makeQuery();
        }
        if (randomV >= t1Floor) {
            return template1.makeQuery();
        }
        return null;
    }
}
