package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataSource
{
    private final String name;

    public DataSource(String name)
    {
        this.name = name;
    }

    public abstract Message read();

    public String getName()
    {
        return name;
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    @Override
    public abstract String toString();
}
