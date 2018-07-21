package cn.edu.ruc.iir.paraflow.loader.producer;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * collector runtime
 *
 * @author guodong
 */
public class CollectorRuntime
{
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private static Map<String, ListenableFuture> flowFutures = new HashMap<>();

    private CollectorRuntime()
    {}

    public static <T> void run(DataFlow<T> dataFlow)
    {
        FlowTask<T> task = new FlowTask<>(dataFlow);
        executorService.submit(() -> {
            flowFutures.put(dataFlow.getName(), task.execute());
        });
    }
}
