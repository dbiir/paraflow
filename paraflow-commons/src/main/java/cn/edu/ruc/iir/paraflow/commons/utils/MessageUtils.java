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
     * timestamp(8 bytes) + fiberId(4 bytes) + keyIndex(4 bytes) + num of values(4 bytes)
     *   + [value length(4 bytes) + value content(m bytes)] repeat for k values
     *   + topic value length + topic
     * */
    public static byte[] toBytes(Message msg) throws MessageSerializeException
    {
        int bytesSize = Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES;
        if (!msg.getTimestamp().isPresent()) {
            throw new MessageSerializeException("Message timestamp is not present");
        }
        if (!msg.getFiberId().isPresent()) {
            throw new MessageSerializeException("Message fiber id is not present");
        }
        if (!msg.getTopic().isPresent()) {
            throw new MessageSerializeException("Message topic is not present");
        }
        for (String v : msg.getValues()) {
            bytesSize += Integer.BYTES;
            bytesSize += v.length();
        }
        bytesSize += Integer.BYTES;
        bytesSize += msg.getTopic().get().length();
        ByteBuffer buffer = ByteBuffer.allocate(bytesSize);
        buffer.putLong(msg.getTimestamp().get());
        buffer.putInt(msg.getFiberId().get());
        buffer.putInt(msg.getKeyIndex());
        buffer.putInt(msg.getValues().length);
        for (String v : msg.getValues()) {
            buffer.putInt(v.length());
            buffer.put(v.getBytes());
        }
        buffer.putInt(msg.getTopic().get().length());
        buffer.put(msg.getTopic().get().getBytes());
        return buffer.array();
    }

    public static Message fromBytes(byte[] bytes) throws MessageDeSerializationException
    {
        try {
            ByteBuffer wrapper = ByteBuffer.wrap(bytes);
            long timestamp = wrapper.getLong();
            int fiberId = wrapper.getInt();
            int keyIndex = wrapper.getInt();
            int valueNum = wrapper.getInt();
            String[] values = new String[valueNum];
            for (int i = 0; i < valueNum; i++) {
                int vLen = wrapper.getInt();
                byte[] v = new byte[vLen];
                wrapper.get(v);
                values[i] = new String(v, StandardCharsets.UTF_8);
            }
            int tLen = wrapper.getInt();
            byte[] t = new byte[tLen];
            wrapper.get(t);
            String topic = new String(t, StandardCharsets.UTF_8);
            return new Message(keyIndex, values, timestamp, topic, fiberId);
        }
        catch (Exception e) {
            throw new MessageDeSerializationException();
        }
    }
}
