package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageDeSerializationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageSerializeException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;

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
        return new byte[0];
    }

    public static Message fromBytes(byte[] bytes) throws MessageDeSerializationException
    {
        return null;
    }
}
