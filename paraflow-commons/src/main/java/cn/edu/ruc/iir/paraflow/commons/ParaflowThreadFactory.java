package cn.edu.ruc.iir.paraflow.commons;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowThreadFactory
        implements ThreadFactory
{
    private static final ThreadFactory defaultTF = Executors.defaultThreadFactory();
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    public ParaflowThreadFactory(Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
    {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public Thread newThread(Runnable runnable)
    {
        Thread thread = defaultTF.newThread(runnable);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }
}
