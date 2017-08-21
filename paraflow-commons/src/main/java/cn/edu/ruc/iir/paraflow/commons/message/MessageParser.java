package cn.edu.ruc.iir.paraflow.commons.message;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MethodNotImplementedException;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class MessageParser
{
    public static Message parseFrom(String message) throws MethodNotImplementedException
    {
        throw new MethodNotImplementedException(MessageParser.class.getName() + " parseFrom$0");
    }

    public static Message parseFrom(String message, String delimiter) throws MethodNotImplementedException
    {
        throw new MethodNotImplementedException(MessageParser.class.getName() + " parseFrom$1");
    }

    public static byte[] parseTo(Message message) throws MethodNotImplementedException
    {
        throw new MethodNotImplementedException(MessageParser.class.getName() + " parseTo");
    }
}
