package cn.edu.ruc.iir.paraflow.collector;

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

    public abstract String getDb();

    public abstract String getTbl();

    public abstract DataType getType();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    @Override
    public abstract String toString();

    public enum DataType
    {
        Parquet, ORC;
    }
}
