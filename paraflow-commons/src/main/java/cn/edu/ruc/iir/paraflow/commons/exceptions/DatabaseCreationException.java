package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class DatabaseCreationException extends ParaFlowException
{
    private static final long serialVersionUID = -8371247825004879423L;

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return "Database creation exception";
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus
                .newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.DATABASE_CREATION_ERROR)
                .build();
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     */
    @Override
    public String getHint()
    {
        return "Check and try again";
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.ERROR;
    }
}
