package cn.edu.ruc.iir.paraflow.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * paraflow loader process pipeline

 * @author guodong
 */
public class ProcessPipeline
{
    private final List<Processor> processors;
    private final List<RunningProcessor> runningProcessors;
    private final ExecutorService executorService;

    public ProcessPipeline()
    {
        this.processors = new ArrayList<>();
        this.runningProcessors = new ArrayList<>();
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2);
    }

    public void addProcessor(Processor processor)
    {
        this.processors.add(processor);
    }

    public void start()
    {
        for (Processor processor : processors) {
            for (int i = 0; i < processor.getParallelism(); i++) {
                RunningProcessor runningProcessor = new RunningProcessor(processor);
                executorService.submit(runningProcessor);
                runningProcessors.add(runningProcessor);
            }
        }
    }

    public void stop()
    {
        executorService.shutdown();
        for (RunningProcessor runningProcessor : runningProcessors) {
            runningProcessor.stop();
        }
        executorService.shutdownNow();
    }
}
