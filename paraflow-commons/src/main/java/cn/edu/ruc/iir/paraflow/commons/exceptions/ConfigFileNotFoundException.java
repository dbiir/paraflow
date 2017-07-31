package cn.edu.ruc.iir.paraflow.commons.exceptions;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class ConfigFileNotFoundException extends ParaFlowException
{
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
        return ParaFlowExceptionLevel.WARNING;
    }
}
