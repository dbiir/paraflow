package cn.edu.ruc.iir.paraflow.benchmark.query;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * paraflow
 *
 * @author guodong
 */
public class QueryDistribution
{
    private final int select;
    private final int update;
    private final int insert;
    private final int delete;
    private long sizeLimit = -1L;  // limit of query size generation. when reaching the specified number of queries, stop generation.
    private long timeLimit = -1L;  // limit of query time generation. when reaching the specified time, stop generation.

    public QueryDistribution(int select, int update, int insert, int delete)
    {
        checkArgument(select >= 0, "select should not be negative");
        checkArgument(update >= 0, "update should not be negative");
        checkArgument(insert >= 0, "insert should not be negative");
        checkArgument(delete >= 0, "delete should not be negative");
        this.select = select;
        this.update = update;
        this.insert = insert;
        this.delete = delete;
    }

    public void setSizeLimit(long sizeLimit)
    {
        checkArgument(sizeLimit >= 0, "size limit should not be negative");
        this.sizeLimit = sizeLimit;
    }

    public void setTimeLimit(long timeLimit)
    {
        checkArgument(timeLimit >= 0, "time limit should not be negative");
        this.timeLimit = timeLimit;
    }

    public long sizeLimit()
    {
        return sizeLimit;
    }

    public long timeLimit()
    {
        return timeLimit;
    }

    public int selectValue()
    {
        return select;
    }

    public int updateValue()
    {
        return update;
    }

    public int insertValue()
    {
        return insert;
    }

    public int deleteValue()
    {
        return delete;
    }

    public double selectScale()
    {
        return 1.0d * select / (select + update + insert + delete);
    }

    public double updateScale()
    {
        return 1.0d * select / (select + update + insert + delete);
    }

    public double insertScale()
    {
        return 1.0d * select / (select + update + insert + delete);
    }

    public double deleteScale()
    {
        return 1.0d * select / (select + update + insert + delete);
    }
}
