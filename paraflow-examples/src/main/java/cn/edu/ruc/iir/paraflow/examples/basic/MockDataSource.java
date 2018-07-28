package cn.edu.ruc.iir.paraflow.examples.basic;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.commons.message.Message;

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
        sb.append(key).append(",")
                .append(2 * key).append(",")
                .append("alicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavidalicebobdavid")
                .append(key).append(",")
                .append(timestamp);
        byte[] v = sb.toString().getBytes();
        return new Message(toBytes(key), v, timestamp);
    }

    private byte[] toBytes(int v)
    {
        byte[] result = new byte[4];
        result[0] = (byte) (v >> 24);
        result[1] = (byte) (v >> 16);
        result[2] = (byte) (v >> 8);
        result[3] = (byte) (v >> 0);
        return result;
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
