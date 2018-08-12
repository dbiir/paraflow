package cn.edu.ruc.iir.paraflow.commons;

import java.util.Arrays;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowThreadExceptionHandler
        implements Thread.UncaughtExceptionHandler
{
    @Override
    public void uncaughtException(Thread thread, Throwable t)
    {
        System.err.println("Uncaught exception detected " + t + " st: " + Arrays.toString(t.getStackTrace()));
    }
}
