package cn.edu.ruc.iir.paraflow.benchmark.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

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
        this.name = checkNotNull(name, "name is null");
        this.comment = checkNotNull(comment, "comment is null");
    }

    @Override
    public long getRowNumber()
    {
        return rowNumber;
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

    @Override
    public String toLine()
    {
        return String.format(ENGLISH, "%d|%s|%s", regionKey, name, comment);
    }
}
