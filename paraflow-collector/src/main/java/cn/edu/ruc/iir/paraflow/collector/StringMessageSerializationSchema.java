package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

/**
 * default message serialization schema
 *
 * @author guodong
 */
public class StringMessageSerializationSchema<T>
        implements MessageSerializationSchema<T>
{
    @Override
    public Message serialize(int keyIdx, int timeIdx, T value)
    {
        String valueStr = (String) value;
        String[] values = valueStr.split(",");
        byte[] key = values[keyIdx].getBytes();
        long timestamp = Long.parseLong(values[timeIdx]);
        byte[][] valueBytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            valueBytes[i] = values[i].getBytes();
        }
        return new Message(key, valueBytes, timestamp);
    }
}
