package cn.edu.ruc.iir.paraflow.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * paraflow loader process pipeline

 * @author guodong
 */
class ProcessPipeline
{
    private static final Logger logger = LoggerFactory.getLogger(ProcessPipeline.class);
    private final List<Processor> processors;
    private final List<RunningProcessor> runningProcessors;
    private final ExecutorService executorService;
    private final List<Future> futures = new ArrayList<>();

    ProcessPipeline()
    {
        this.processors = new ArrayList<>();
        this.runningProcessors = new ArrayList<>();
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2);
    }

    void addProcessor(Processor processor)
    {
        this.processors.add(processor);
    }

    void start()
    {
        for (Processor processor : processors) {
            for (int i = 0; i < processor.getParallelism(); i++) {
                RunningProcessor runningProcessor = new RunningProcessor(processor);
                futures.add(executorService.submit(runningProcessor));
                runningProcessors.add(runningProcessor);
            }
        }
        logger.info("Loading pipeline started.");
        while (!futures.isEmpty()) {
            for (Future future : futures) {
                if (future.isDone()) {
                    try {
                        future.get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        // todo deal with execution exceptions
                        e.printStackTrace();
                        futures.remove(future);
                    }
                }
            }
        }
    }

    ExecutorService getExecutorService()
    {
        return this.executorService;
    }

    void stop()
    {
        executorService.shutdown();
        for (RunningProcessor runningProcessor : runningProcessors) {
            runningProcessor.stop();
        }
        executorService.shutdownNow();
    }
}
