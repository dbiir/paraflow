package cn.edu.ruc.iir.paraflow.loader.producer;

import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataSource<T>
{
    private final String name;

    public DataSource(String name)
    {
        this.name = name;
    }

    public abstract T read();

    public abstract T[] readBatch();

    public abstract List<T> readBulk();

    public String getName()
    {
        return name;
    }

    @Override
    public abstract String toString();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);
}
