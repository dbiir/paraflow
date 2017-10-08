package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageDeSerializationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageSerializeException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * paraflow
 *
 * @author guodong
 */
public class MessageUtils
{
    private MessageUtils()
    {}

    /**
     * Default message serialization
     * format:
     * keyIndex(4 bytes) + timestamp(8 bytes) + num of values(4 bytes) + [value length(4 bytes) + value content(m bytes)] repeat for k values
     * */
    public static byte[] toBytes(Message msg) throws MessageSerializeException
    {
        int bytesSize = Integer.BYTES + Long.BYTES + Integer.BYTES;
        if (!msg.getTimestamp().isPresent()) {
            throw new MessageSerializeException("Message timestamp is not present");
        }
        for (String v : msg.getValues()) {
            bytesSize += Integer.BYTES;
            bytesSize += v.length();
        }
        ByteBuffer buffer = ByteBuffer.allocate(bytesSize);
        buffer.putInt(msg.getKeyIndex());
        buffer.putLong(msg.getTimestamp().get());
        buffer.putInt(msg.getValues().length);
        for (String v : msg.getValues()) {
            buffer.putInt(v.length());
            buffer.put(v.getBytes());
        }

        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) throws MessageDeSerializationException
    {
        try {
            ByteBuffer wrapper = ByteBuffer.wrap(bytes);
            int keyIndex = wrapper.getInt();
            long timestamp = wrapper.getLong();
            int valueNum = wrapper.getInt();
            String[] values = new String[valueNum];
            for (int i = 0; i < valueNum; i++) {
                int vLen = wrapper.getInt();
                byte[] v = new byte[vLen];
                wrapper.get(v);
                values[i] = new String(v, StandardCharsets.UTF_8);
            }
            return new Message(keyIndex, values, timestamp);
        }
        catch (Exception e) {
            throw new MessageDeSerializationException();
        }
    }
}
