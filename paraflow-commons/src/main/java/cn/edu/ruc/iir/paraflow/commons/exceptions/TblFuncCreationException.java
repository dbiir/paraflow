package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class TblFuncCreationException extends ParaFlowException
{
    @Override
    public String getMessage()
    {
        return null;
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.TBL_FUNC_CREATION_ERROR)
                        .build();
    }

    @Override
    public String getHint()
    {
        return null;
    }

    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.ERROR;
    }
}
