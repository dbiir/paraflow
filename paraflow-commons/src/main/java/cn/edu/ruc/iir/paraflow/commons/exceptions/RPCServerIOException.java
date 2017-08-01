package cn.edu.ruc.iir.paraflow.commons.exceptions;

/**
 * This exception is intended to be used in paraflow-metaserver.
 * Representing for gRPC server IOExpception when starting up.
 *
 * @author guodong
 */
public class RPCServerIOException extends ParaFlowException
{
    private static final long serialVersionUID = 5621165837126253248L;

    private final int port;

    public RPCServerIOException(int port)
    {
        this.port = port;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("gRPC server cannot start at port %d", port);
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     */
    @Override
    public String getHint()
    {
        return "Server stops. Check if port is already used and try again.";
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
