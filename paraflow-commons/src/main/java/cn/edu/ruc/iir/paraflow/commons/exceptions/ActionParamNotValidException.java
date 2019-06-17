package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

//todo alice: add exception implementation
public class ActionParamNotValidException extends ParaFlowException
{
    /**
     * get error message.
     *
     * @return error message
     */
    private static final long serialVersionUID = 6980171393859140122L;

    @Override
    public String getMessage()
    {
        return String.format("Action param not valid !");
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus
                .newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.ACTION_PARAM_INVALID_ERROR)
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
        return "Action param not valid !";
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
