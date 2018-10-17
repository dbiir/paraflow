package cn.edu.ruc.iir.paraflow.collector;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataSink
{
    public enum DataType
    {
        Parquet, ORC;
    }

    private final String name;

    public DataSink(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public abstract String getDb();

    public abstract String getTbl();

    public abstract DataType getType();

    @Override
    public abstract String toString();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);
}
