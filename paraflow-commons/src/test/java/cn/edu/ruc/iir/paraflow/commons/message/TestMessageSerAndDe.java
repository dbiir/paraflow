package cn.edu.ruc.iir.paraflow.commons.message;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageDeSerializationException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.MessageSerializeException;
import cn.edu.ruc.iir.paraflow.commons.utils.MessageUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestMessageSerAndDe
{
    @Test
    public void serAndDeTest()
    {
        long timestamp = System.currentTimeMillis();
        String[] values = {"test0", "and", "test1", "try", "ser", "der"};
        Message expected = new Message(0, values, timestamp);
        expected.setFiberId(1);
        try {
            byte[] serBytes = MessageUtils.toBytes(expected);
            Message result = MessageUtils.fromBytes(serBytes);
            assertEquals(expected, result);
        }
        catch (MessageSerializeException | MessageDeSerializationException e) {
            e.printStackTrace();
        }
    }
}
