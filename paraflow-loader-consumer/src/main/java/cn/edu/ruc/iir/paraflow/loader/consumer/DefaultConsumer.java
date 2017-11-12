package cn.edu.ruc.iir.paraflow.loader.consumer;

import cn.edu.ruc.iir.paraflow.commons.TopicFiber;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.consumer.threads.DataThreadManager;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class DefaultConsumer implements Consumer
{
    private final AdminClient kafkaAdminClient;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private List<TopicPartition> topicPartitions;
    private List<TopicFiber> topicFibers;
    private final DataThreadManager dataThreadManager;

    public DefaultConsumer(String configPath, List<TopicPartition> topicPartitions, List<TopicFiber> topicFibers) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
        this.topicPartitions = topicPartitions;
        this.topicFibers = topicFibers;
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
        this.dataThreadManager = DataThreadManager.INSTANCE();
        init();
    }

    private void init()
    {
        dataThreadManager.init(topicPartitions, topicFibers);
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(DataThreadManager.INSTANCE()::shutdown)
        );
    }

    public void consume()
    {
        dataThreadManager.run();
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
