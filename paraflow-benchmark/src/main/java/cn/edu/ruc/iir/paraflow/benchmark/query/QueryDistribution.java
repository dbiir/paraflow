package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * paraflow
 *
 * @author guodong
 */
public class QueryDistribution
{
    private final Map<String, Long> distributions;
    private long sizeLimit = -1L;  // limit of query size generation. when reaching the specified number of queries, stop generation.
    private long timeLimit = -1L;  // limit of query time generation. when reaching the specified time, stop generation.

    public QueryDistribution()
    {
        this.distributions = new HashMap<>();
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

    public void setDistribution(String key, long distributeValue)
    {
        distributions.put(key, distributeValue);
    }

    public long getValue(String key)
    {
        return distributions.getOrDefault(key, 0L);
    }
}
