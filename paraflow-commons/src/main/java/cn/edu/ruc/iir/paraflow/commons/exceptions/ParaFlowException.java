package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * ParaFlow
 *
 * @author guodong
 */
public abstract class ParaFlowException extends Exception
{
    private static final long serialVersionUID = -6514778398567346776L;
    private static final Logger logger = LogManager.getLogger(ParaFlowException.class);

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

    public void handle()
    {
        switch (getLevel()) {
            case DEBUG:
                System.out.println(toString());
                return;
            case INFO:
                logger.log(Level.INFO, toString());
                return;
            case WARN:
                logger.log(Level.WARN, toString());
                return;
            case ERROR:
                logger.log(Level.ERROR, getStackTraceMessage());
                return;
            case FATAL:
                logger.log(Level.FATAL, getStackTraceMessage());
                Runtime.getRuntime().exit(getResponseStatus().getStatusValue());
        }
    }

    public abstract StatusProto.ResponseStatus getResponseStatus();

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

    private String getStackTraceMessage()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        super.printStackTrace(pw);

        return sw.toString();
    }
}
