package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.threads.DataThreadManager;
import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;
import java.util.function.Function;

public class DefaultLoader
{
    private final AdminClient kafkaAdminClient;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private ProcessPipeline pipeline;

    public DefaultLoader(String configPath) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        props.setProperty("client.id", "consumerAdmin");
        props.setProperty("metadata.max.age.ms", "3000");
        props.setProperty("group.id", config.getGroupId());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("session.timeout.ms", "30000");
        kafkaAdminClient = AdminClient.create(props);
        this.pipeline = new ProcessPipeline();
        init();
    }

    private void init()
    {
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(DataThreadManager.INSTANCE()::shutdown)
        );
        // construct default pipeline
    }

    public void consume()
    {
        pipeline.start();
    }

    public void registerFiberFunc(String database, String table, Function<String, Integer> fiberFunc)
    {
        funcMapBuffer.put(FormTopicName.formTopicName(database, table), fiberFunc);
    }

    private void beforeShutdown()
    {
        kafkaAdminClient.close();
        pipeline.stop();
    }
}
