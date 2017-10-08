package cn.edu.ruc.iir.paraflow.commons.message;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageDeSerializationException;
import cn.edu.ruc.iir.paraflow.commons.utils.MessageUtils;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultMessageDe extends MessageDe
{
    @Override
    public Message deserialize(String topic, byte[] data)
    {
        try {
            return MessageUtils.fromBytes(data);
        }
        catch (MessageDeSerializationException e) {
            e.printStackTrace();
            return null;
        }
    }
}
