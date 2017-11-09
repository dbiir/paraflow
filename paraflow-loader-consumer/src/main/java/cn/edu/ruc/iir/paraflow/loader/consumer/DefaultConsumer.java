package cn.edu.ruc.iir.paraflow.loader.consumer;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.consumer.threads.DataProcessThreadManager;
import cn.edu.ruc.iir.paraflow.loader.consumer.threads.DataPullThreadManager;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class DefaultConsumer implements Consumer
{
    private final AdminClient kafkaAdminClient;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private List<TopicPartition> topicPartitions = new LinkedList<>();
    private DataPullThreadManager dataPullThreadManager;
    private DataProcessThreadManager dataProcessThreadManager;

    public DefaultConsumer(String configPath, List<TopicPartition> topicPartitions) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
        this.topicPartitions = topicPartitions;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        props.setProperty("client.id", "consumerAdmin");
        props.setProperty("metadata.max.age.ms", "3000");
        props.setProperty("group.id", config.getGroupId());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("key.deserializer", config.getKafkaKeyDeserializerClass());
        props.setProperty("value.deserializer", config.getKafkaValueDeserializerClass());
        kafkaAdminClient = AdminClient.create(props);
        dataPullThreadManager = DataPullThreadManager.INSTANCE();
        dataProcessThreadManager = DataProcessThreadManager.INSTANCE();
        init();
    }

    private void init()
    {
        dataPullThreadManager.init(topicPartitions);
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(DataPullThreadManager.INSTANCE()::shutdown)
        );
        dataPullThreadManager.run();
    }

    public void consume()
    {
        dataProcessThreadManager.init(topicPartitions);
        Runtime.getRuntime().addShutdownHook(
                new Thread(DataProcessThreadManager.INSTANCE()::shutdown)
        );
        dataProcessThreadManager.run();
    }

    public void registerFiberFunc(String database, String table, Function<String, Integer> fiberFunc)
    {
        funcMapBuffer.put(FormTopicName.formTopicName(database, table), fiberFunc);
    }

    private void beforeShutdown()
    {
        kafkaAdminClient.close();
    }

    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }
}
