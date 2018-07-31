package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataThread implements Runnable
{
    String threadName;
    boolean isReadyToStop = false;

    public DataThread(String threadName)
    {
        this.threadName = threadName;
    }

    void readyToStop()
    {
        this.isReadyToStop = true;
    }

    public String getName()
    {
        return threadName;
    }

    public void shutdown()
    {
        readyToStop();
    }
}
