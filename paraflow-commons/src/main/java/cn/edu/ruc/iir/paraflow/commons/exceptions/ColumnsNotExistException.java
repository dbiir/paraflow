package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class ColumnsNotExistException extends ParaFlowException
{
    private static final long serialVersionUID = 5600165987123351243L;

    private final String dbName;
    private final String tblName;

    public ColumnsNotExistException(String dbName, String tblName)
    {
        this.dbName = dbName;
        this.tblName = tblName;
    }

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

    @Override
    public String getHint()
    {
        return null;
    }

    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return null;
    }
}
