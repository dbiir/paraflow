package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
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
 */
class ProcessPipeline
{
    private static final Logger logger = LoggerFactory.getLogger(ProcessPipeline.class);
    private final List<Processor> processors;
    private final List<RunningProcessor> runningProcessors;
    private final ExecutorService executorService;
    private final List<Future> futures = new ArrayList<>();

    ProcessPipeline(LoaderConfig config)
    {
        this.processors = new ArrayList<>();
        this.runningProcessors = new ArrayList<>();
        int loaderParallelism;
        if (config.getLoaderParallelism() < 0) {
            loaderParallelism = Runtime.getRuntime().availableProcessors() * 2;
        }
        else {
            loaderParallelism = config.getLoaderParallelism();
        }
        this.executorService = Executors.newFixedThreadPool(loaderParallelism);
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
        int finishedProcessors = 0;
        while (finishedProcessors < futures.size()) {
            for (Future future : futures) {
                if (future.isDone() || future.isCancelled()) {
                    try {
                        future.get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        // todo deal with execution exceptions
                        e.printStackTrace();
                    }
                    finishedProcessors++;
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
        SegmentContainer.INSTANCE().stop();
        for (RunningProcessor runningProcessor : runningProcessors) {
            runningProcessor.stop();
        }
        executorService.shutdownNow();
    }
}
