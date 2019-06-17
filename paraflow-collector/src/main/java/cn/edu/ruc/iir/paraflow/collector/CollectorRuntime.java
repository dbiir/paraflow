package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.collector.utils.CollectorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * collector runtime
 *
 * @author guodong
 */
class CollectorRuntime
{
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final Map<String, FlowTask> flowTasks = new HashMap<>();
    private static final long statsInterval = 3000L;
    private static ParaflowKafkaProducer kafkaProducer;

    CollectorRuntime(CollectorConfig conf)
    {
        kafkaProducer = new ParaflowKafkaProducer(conf, statsInterval);
    }

    static void destroy()
    {
        for (FlowTask task : flowTasks.values()) {
            task.close();
        }
        executorService.shutdownNow();
    }

    <T> void run(DataFlow<T> dataFlow)
    {
        FlowTask<T> task = new FlowTask<>(dataFlow, kafkaProducer);
        flowTasks.put(dataFlow.getName(), task);
        executorService.submit((Runnable) task::execute);
    }
}
