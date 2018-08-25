package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow
 *
 * @author guodong
 */
public class PrestoQueryGenerator
        extends QueryGenerator
{
    private final PrestoSelectTemplate selectTemplate;
    private long counter = 0;

    public PrestoQueryGenerator(QueryDistribution distribution)
    {
        super(distribution);
        this.selectTemplate = new PrestoSelectTemplate();
    }

    @Override
    public boolean hasNext()
    {
        return counter < distribution.sizeLimit();
    }

    @Override
    public String next()
    {
        counter++;
        return selectTemplate.makeQuery();
    }
}
