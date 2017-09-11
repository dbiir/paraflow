package cn.edu.ruc.iir.paraflow.commons.message;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class MessageDe implements Deserializer<Message>
{
    @Override
    public void configure(Map map, boolean b)
    {
        // nothing to do
    }

    @Override
    public void close()
    {
        // nothing to do
    }
}
