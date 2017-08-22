package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class MetaTableCorruptedException extends ParaFlowException
{
    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return null;
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return null;
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     */
    @Override
    public String getHint()
    {
        return null;
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return null;
    }
}
