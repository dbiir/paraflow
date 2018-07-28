package cn.edu.ruc.iir.paraflow.examples.basic;

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
    public MockDataSink()
    {
        super("mock-sink");
    }

    @Override
    public String getDb()
    {
        return "test";
    }

    @Override
    public String getTbl()
    {
        return "mock";
    }

    @Override
    public DataType getType()
    {
        return DataType.Parquet;
    }

    @Override
    public String toString()
    {
        return "MockDataSink";
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
}
