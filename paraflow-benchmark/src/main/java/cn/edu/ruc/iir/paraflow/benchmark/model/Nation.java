package cn.edu.ruc.iir.paraflow.benchmark.model;

import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class Nation
        implements Model
{
    private final long rowNumber;
    private final long nationKey;
    private final String name;
    private final long regionKey;
    private final String comment;

    public Nation(long rowNumber, long nationKey, String name, long regionKey, String comment)
    {
        this.rowNumber = rowNumber;
        this.nationKey = nationKey;
        this.name = Objects.requireNonNull(name, "name is null");
        this.regionKey = regionKey;
        this.comment = Objects.requireNonNull(comment, "comment is null");
    }

    @Override
    public long getRowNumber()
    {
        return rowNumber;
    }

    @Override
    public String toLine()
    {
        return nationKey + "|" +
                name + "|" +
                regionKey + "|" +
                comment;
    }

    public long getNationKey()
    {
        return nationKey;
    }

    public String getName()
    {
        return name;
    }

    public long getRegionKey()
    {
        return regionKey;
    }

    public String getComment()
    {
        return comment;
    }
}
