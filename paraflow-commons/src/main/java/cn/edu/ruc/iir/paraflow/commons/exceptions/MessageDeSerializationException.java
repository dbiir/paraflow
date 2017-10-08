package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class MessageDeSerializationException extends ParaFlowException
{
    private static final long serialVersionUID = -6499213164635187737L;

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return "Message deserialization exception";
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        // todo set status as MSG_DESERIALIZE_ERROR
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
        return "Check byte content and this one is ignored";
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
