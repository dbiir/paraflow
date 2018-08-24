package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.BytesUtils;

import java.util.Objects;
import java.util.Random;

/**
 * paraflow
 *
 * @author guodong
 */
public class MockDataSource
        extends DataSource
{
    public MockDataSource()
    {
        super("mock-source");
    }

    private Random random = new Random();

    @Override
    public Message read()
    {
        int key = random.nextInt(1000);
        long timestamp = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        // key<int>, v1<int>, v2<string>, time<long>
        sb.append(key).append(",")
                .append(2 * key).append(",")
                .append("alicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavid")
                .append(key).append(",")
                .append(timestamp);
        byte[] v = sb.toString().getBytes();
        return new Message(BytesUtils.toBytes(key), v, timestamp);
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
