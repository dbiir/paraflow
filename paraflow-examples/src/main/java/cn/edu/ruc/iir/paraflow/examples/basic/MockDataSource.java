package cn.edu.ruc.iir.paraflow.examples.basic;

import cn.edu.ruc.iir.paraflow.loader.producer.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class MockDataSource
        extends DataSource<String>
{
    public MockDataSource()
    {
        super("mock-source");
    }

    @Override
    public String read()
    {
        return String.valueOf(System.currentTimeMillis());
    }

    @Override
    public String[] readBatch()
    {
        String[] batch = new String[10];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = String.valueOf(System.currentTimeMillis() >> i);
        }
        return batch;
    }

    @Override
    public List<String> readBulk()
    {
        List<String> bulk = new ArrayList<>(10);
        for (int i = 0; i < bulk.size(); i++) {
             bulk.add(i, String.valueOf(System.currentTimeMillis() >> i));
        }
        return bulk;
    }

    @Override
    public String toString()
    {
        return "MockDataSource";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode("mock-source");
    }

    @Override
    public boolean equals(Object other)
    {
        return false;
    }
}
