package cn.edu.ruc.iir.paraflow.collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
    private static Map<String, FlowTask> flowTasks = new HashMap<>();

    private CollectorRuntime()
    {}

    public static <T> void run(DataFlow<T> dataFlow, Properties conf)
    {
        FlowTask<T> task = new FlowTask<>(dataFlow, conf);
        flowTasks.put(dataFlow.getName(), task);
        executorService.submit((Runnable) task::execute);
    }

    public static void destroy()
    {
        for (FlowTask task : flowTasks.values()) {
            task.close();
        }
        executorService.shutdownNow();
    }
}
