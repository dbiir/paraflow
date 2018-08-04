package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.collector.utils.CollectorConfig;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultCollector<T>
        implements Collector<T>
{
    private final MetaClient metaClient;
    private final AdminClient kafkaAdminClient;
    private final CollectorEnvironment env = CollectorEnvironment.getEnvironment();
    private final CollectorConfig collectorConfig;

    public DefaultCollector()
            throws ConfigFileNotFoundException
    {
        CollectorConfig config = CollectorConfig.INSTANCE();
        config.init();
        this.collectorConfig = config;
        // init meta client
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        // init kafka admin client
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        properties.setProperty("client.id", "producerAdmin");
        properties.setProperty("metadata.max.age.ms", "3000");
        kafkaAdminClient = AdminClient.create(properties);
        init();
    }

    private void init()
    {
        // register shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(CollectorRuntime::destroy)
        );
    }

    @Override
    public void collect(DataSource dataSource, int keyIdx, int timeIdx,
                        ParaflowFiberPartitioner partitioner,
                        MessageSerializationSchema<T> serializer,
                        DataSink dataSink)
    {
        DataFlow<T> dataFlow = env.addSource(dataSource);
        FiberFlow<T> fiberFlow = dataFlow
                .keyBy(keyIdx)
                .timeBy(timeIdx)
                .sink(dataSink)
                .partitionBy(partitioner)
                .serializeBy(serializer);
        CollectorRuntime.run(fiberFlow, collectorConfig.getProperties());
    }

    @Override
    public void createTopic(String topicName, int partitionsNum, short replicationFactor)
    {
        NewTopic newTopic = new NewTopic(topicName, partitionsNum, replicationFactor);
        CreateTopicsResult result = kafkaAdminClient.createTopics(Collections.singletonList(newTopic));
        KafkaFuture future = result.values().get(topicName);
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteTopic(String topicName)
    {
        Collection<String> topics = new LinkedList<>();
        topics.add(topicName);
        kafkaAdminClient.deleteTopics(topics);
    }

    @Override
    public StatusProto.ResponseStatus createUser(String userName, String password)
    {
        return metaClient.createUser(userName, password);
    }

    @Override
    public StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl)
    {
        return metaClient.createDatabase(databaseName, userName, locationUrl);
    }

    @Override
    public StatusProto.ResponseStatus createRegularTable(
            String dbName,
            String tblName,
            String userName,
            String locationUrl,
            String storageFormatName,
            List<String> columnName,
            List<String> dataType)
    {
        return metaClient.createRegularTable(dbName, tblName, userName, locationUrl, storageFormatName, columnName, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName,
                                                       String tblName,
                                                       String userName,
                                                       String storageFormatName,
                                                       int fiberColIndex,
                                                       int timestampColIndex,
                                                       String fiberFuncName,
                                                       List<String> columnName,
                                                       List<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColIndex, fiberFuncName, timestampColIndex, columnName, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createFiberTable(String dbName,
                                                       String tblName,
                                                       String userName,
                                                       String locationUrl,
                                                       String storageFormatName,
                                                       int fiberColIndex,
                                                       int timestampColIndex,
                                                       String fiberFuncName,
                                                       List<String> columnName,
                                                       List<String> dataType)
    {
        return metaClient.createFiberTable(dbName, tblName, userName, storageFormatName, fiberColIndex, fiberFuncName, timestampColIndex, columnName, dataType);
    }

    @Override
    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }

    private void beforeShutdown()
    {
        kafkaAdminClient.close();
        try {
            metaClient.shutdown(1);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
