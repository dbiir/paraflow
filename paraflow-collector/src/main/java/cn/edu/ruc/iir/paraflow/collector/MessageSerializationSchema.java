package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;

import java.io.Serializable;

/**
 * paraflow
 *
 * @author guodong
 */
public interface MessageSerializationSchema<T>
        extends Serializable
{
    Message serialize(int keyIdx, int timeIdx, T value);
}
