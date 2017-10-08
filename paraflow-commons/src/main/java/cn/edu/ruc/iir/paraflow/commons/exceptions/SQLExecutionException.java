package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class SQLExecutionException extends ParaFlowException
{
    private static final long serialVersionUID = 5939773817133884884L;
    private final String sql;

    public SQLExecutionException(String sql)
    {
        this.sql = sql;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("SQL execution error [%s]", sql);
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.SQL_EXECUTION_ERROR)
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
        return "jdbc error";
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
