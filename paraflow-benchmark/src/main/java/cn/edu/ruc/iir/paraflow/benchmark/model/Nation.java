package cn.edu.ruc.iir.paraflow.benchmark.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

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
        this.name = checkNotNull(name, "name is null");
        this.regionKey = regionKey;
        this.comment = checkNotNull(comment, "comment is null");
    }

    @Override
    public long getRowNumber()
    {
        return rowNumber;
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

    @Override
    public String toLine()
    {
        return String.format(ENGLISH, "%d|%s|%d|%s", nationKey, name, regionKey, comment);
    }
}
