package cn.edu.ruc.iir.paraflow.commons.message;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageSerializeException;
import cn.edu.ruc.iir.paraflow.commons.utils.MessageUtils;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultMessageSer extends MessageSer
{
    @Override
    public byte[] serialize(String topic, Message msg)
    {
        try {
            return MessageUtils.toBytes(msg);
        }
        catch (MessageSerializeException e) {
            e.printStackTrace();
            return new byte[1];
        }
    }
}
