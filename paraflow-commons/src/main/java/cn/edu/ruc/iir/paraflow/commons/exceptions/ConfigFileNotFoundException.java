package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

/**
 * ParaFlow
 *
 * @author guodong
 */
public final class ConfigFileNotFoundException extends ParaFlowException
{
    private static final long serialVersionUID = 6980171393859140121L;

    private final String configPath;

    public ConfigFileNotFoundException(String configPath)
    {
        this.configPath = configPath;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("Configuration file %s not found", this.configPath);
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus
                .newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.CONFIG_FILE_NOT_FOUND_FATAL)
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
        return "Use default configuration properties set by user or in system instead.";
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
