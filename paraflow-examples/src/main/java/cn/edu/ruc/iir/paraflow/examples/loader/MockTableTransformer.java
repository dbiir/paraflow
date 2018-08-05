package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.loader.DataTransformer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowRecord;

/**
 * paraflow
 *
 * @author guodong
 */
public class MockTableTransformer
        implements DataTransformer
{
    @Override
    public ParaflowRecord transform(byte[] value, int partition)
    {
        String valueStr = new String(value);
        String[] valueParts = valueStr.split(",");
        int key = Integer.parseInt(valueParts[0]);
        int v1 = Integer.parseInt(valueParts[1]);
        String v2 = valueParts[2];
        long timestamp = Long.parseLong(valueParts[3]);

        return new ParaflowRecord(key, timestamp, partition, key, v1, v2, timestamp);
    }
}
