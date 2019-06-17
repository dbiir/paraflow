package cn.edu.ruc.iir.paraflow.loader;

public class RunningProcessor
        implements Runnable
{
    private final Processor processor;

    public RunningProcessor(Processor processor)
    {
        this.processor = processor;
    }

    @Override
    public void run()
    {
        processor.run();
    }

    public void stop()
    {
        processor.stop();
    }
}
