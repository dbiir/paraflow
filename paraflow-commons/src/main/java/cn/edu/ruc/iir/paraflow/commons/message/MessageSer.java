package cn.edu.ruc.iir.paraflow.commons.message;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class MessageSer implements Serializer<Message>
{
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {
        // nothing to do
    }

    @Override
    public void close()
    {
        // nothing to do
    }
}
