package cn.edu.ruc.iir.paraflow.loader;

import com.lmax.disruptor.dsl.Disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * paraflow loader process pipeline
 * basic pipeline setup: DataPuller -> DataTransformer -> DataCompactor -> DataFlusher
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
        ThreadFactory threadFactory = r -> new Thread(r, "paraflow-thread");
        Disruptor<ParaflowRecord> disruptor = new Disruptor<>(
                ParaflowRecord::new, 1024, threadFactory);
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
