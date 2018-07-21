package cn.edu.ruc.iir.paraflow.loader.producer;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataSink
{
    private final String name;

    public DataSink(String name)
    {
        this.name = name;
    }

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
