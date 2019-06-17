package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class TblFuncCreationException extends ParaFlowException
{
    private static final long serialVersionUID = 2586643482592134064L;

    @Override
    public String getMessage()
    {
        return "Table function creation exception";
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
        return "Check and try again";
    }

    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.ERROR;
    }
}
