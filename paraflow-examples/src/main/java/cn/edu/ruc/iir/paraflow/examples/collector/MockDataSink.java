package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.collector.DataSink;

import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class MockDataSink
        extends DataSink
{
    private final String dbName;
    private final String tblName;

    public MockDataSink(String dbName, String tblName)
    {
        super("mock-sink");
        this.dbName = dbName;
        this.tblName = tblName;
    }

    @Override
    public String getDb()
    {
        return dbName;
    }

    @Override
    public String getTbl()
    {
        return tblName;
    }

    @Override
    public DataType getType()
    {
        return DataType.Parquet;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode("mock-sink");
    }

    @Override
    public boolean equals(Object other)
    {
        return false;
    }

    @Override
    public String toString()
    {
        return "MockDataSink";
    }
}
