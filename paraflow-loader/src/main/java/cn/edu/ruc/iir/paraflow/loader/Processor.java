package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Processor
{
    private final int parallelism;
    final String name;
    AtomicBoolean isReadyToStop = new AtomicBoolean(false);

    public Processor(String name, int parallelism)
    {
        this.name = name;
        this.parallelism = parallelism;
    }

    public abstract void run();

    public String getName()
    {
        return name;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public void stop()
    {
        isReadyToStop.set(true);
    }
}
