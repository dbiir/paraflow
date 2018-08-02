package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Processor
        implements Runnable
{
    String threadName;
    AtomicBoolean isReadyToStop = new AtomicBoolean(false);

    public Processor(String threadName)
    {
        this.threadName = threadName;
    }

    void readyToStop()
    {
        this.isReadyToStop.set(true);
    }

    public String getName()
    {
        return threadName;
    }

    public void stop()
    {
        isReadyToStop.set(true);
    }
}
