package cn.edu.ruc.iir.paraflow.commons.exceptions;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * ParaFlow
 *
 * @author guodong
 */
public abstract class ParaFlowException extends Exception
{
    /**
     * get exception name. default to class name
     *
     * @return exception name
     * */
    public String getName()
    {
        return this.getClass().getName();
    }

    /**
     * get error message.
     *
     * @return error message
     * */
    public abstract String getMessage();

    @Override
    public StackTraceElement[] getStackTrace()
    {
        return super.getStackTrace();
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     * */
    public abstract String getHint();

    /**
     * get exception level
     *
     * @return exception level
     * */
    public abstract ParaFlowExceptionLevel getLevel();

    @Override
    public String toString()
    {
        return String.format("[%s]%s: %s. %s", getLevel(), getName(), getMessage(), getHint());
    }

    public String getFullMessage()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        super.printStackTrace(pw);

        return sw.toString();
    }
}
