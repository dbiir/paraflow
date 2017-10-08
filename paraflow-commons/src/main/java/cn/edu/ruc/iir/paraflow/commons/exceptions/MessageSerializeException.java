package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class MessageSerializeException extends ParaFlowException
{
    private static final long serialVersionUID = 2583464045707403311L;
    private final String message;

    public MessageSerializeException(String message)
    {
        this.message = message;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return message;
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        // todo set status as MSG_SERIALIZE_ERROR
        return StatusProto.ResponseStatus.newBuilder().build();
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     */
    @Override
    public String getHint()
    {
        return "Check message and this one is ignored";
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.WARN;
    }
}
