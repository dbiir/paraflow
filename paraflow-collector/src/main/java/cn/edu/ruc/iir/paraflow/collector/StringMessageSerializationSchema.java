package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;

/**
 * default message serialization schema
 *
 * @author guodong
 */
public class StringMessageSerializationSchema<T>
        implements MessageSerializationSchema<T>
{
    private static final long serialVersionUID = 7172445062380483822L;

    @Override
    public Message serialize(int keyIdx, int timeIdx, T value)
    {
        return null;
    }
}
