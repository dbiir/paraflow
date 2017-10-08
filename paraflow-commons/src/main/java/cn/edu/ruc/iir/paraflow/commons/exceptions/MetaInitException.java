package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class MetaInitException extends ParaFlowException
{
    private static final long serialVersionUID = -4623517898557801064L;

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return "Metadata initialization error";
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.META_INIT_FATAL)
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
        return "Try clean meta data and try again";
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.FATAL;
    }
}
