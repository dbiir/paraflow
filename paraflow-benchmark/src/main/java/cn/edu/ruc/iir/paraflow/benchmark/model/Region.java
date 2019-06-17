package cn.edu.ruc.iir.paraflow.benchmark.model;

import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class Region
        implements Model
{
    private final long rowNumber;
    private final long regionKey;
    private final String name;
    private final String comment;

    public Region(long rowNumber, long regionKey, String name, String comment)
    {
        this.rowNumber = rowNumber;
        this.regionKey = regionKey;
        this.name = Objects.requireNonNull(name, "name is null");
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
        return regionKey + "|" +
                name + "|" +
                comment;
    }

    public long getRegionKey()
    {
        return regionKey;
    }

    public String getName()
    {
        return name;
    }

    public String getComment()
    {
        return comment;
    }
}
