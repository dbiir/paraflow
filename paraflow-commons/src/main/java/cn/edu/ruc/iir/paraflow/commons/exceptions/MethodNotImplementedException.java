package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public final class MethodNotImplementedException extends ParaFlowException
{
    private static final long serialVersionUID = 2492705799817499827L;
    private final String methodName;

    public MethodNotImplementedException(String methodName)
    {
        this.methodName = methodName;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("Method %s not implemented", methodName);
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.METHOD_NOT_IMPLEMENTED_INFO)
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
        return "Please define method implementation.";
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.INFO;
    }
}
