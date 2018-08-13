package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * paraflow
 *
 * @author guodong
 */
public class UpdateBlockPathException extends ParaFlowException
{
    @Override
    public String getMessage()
    {
        return "update block path exception";
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.BLOCK_PATH_UPDATE_ERROR)
                .build();
    }

    @Override
    public String getHint()
    {
        return "Check and try again";
    }

    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.ERROR;
    }
}
