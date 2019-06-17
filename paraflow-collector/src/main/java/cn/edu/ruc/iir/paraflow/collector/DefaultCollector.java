package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.collector.utils.CollectorConfig;
import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
    private final CollectorRuntime collectorRuntime;

    public DefaultCollector()
            throws ConfigFileNotFoundException
    {
        CollectorConfig config = CollectorConfig.INSTANCE();
        config.init();
        // init meta client
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        // init kafka admin client
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        properties.setProperty("client.id", "producerAdmin");
        properties.setProperty("metadata.max.age.ms", "3000");
        kafkaAdminClient = AdminClient.create(properties);
        this.collectorRuntime = new CollectorRuntime(config);
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
                .partitionBy(partitioner);
        collectorRuntime.run(fiberFlow);
    }

    @Override
    public void deleteTopic(String topicName)
    {
        Collection<String> topics = new LinkedList<>();
        topics.add(topicName);
        kafkaAdminClient.deleteTopics(topics);
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
    public boolean existsDatabase(String db)
    {
        MetaProto.StringListType dbs = metaClient.listDatabases();
        boolean dbExisted = false;
        for (int i = 0; i < dbs.getStrCount(); i++) {
            if (dbs.getStr(i).equals(db)) {
                dbExisted = true;
                break;
            }
        }
        return dbExisted;
    }

    @Override
    public boolean existsTable(String db, String table)
    {
        MetaProto.StringListType tbls = metaClient.listTables(db);
        boolean tblExisted = false;
        for (int i = 0; i < tbls.getStrCount(); i++) {
            if (tbls.getStr(i).equals(table)) {
                tblExisted = true;
                break;
            }
        }
        return tblExisted;
    }

    @Override
    public StatusProto.ResponseStatus createUser(String userName, String password)
    {
        return metaClient.createUser(userName, password);
    }

    @Override
    public StatusProto.ResponseStatus createDatabase(String databaseName)
    {
        return metaClient.createDatabase(databaseName);
    }

    @Override
    public StatusProto.ResponseStatus createDatabase(String databaseName, String userName, String locationUrl)
    {
        return metaClient.createDatabase(databaseName, userName, locationUrl);
    }

    @Override
    public StatusProto.ResponseStatus createTable(String dbName,
                                                  String tblName,
                                                  String storageFormatName,
                                                  int fiberColIndex,
                                                  int timestampColIndex,
                                                  String fiberPartitioner,
                                                  List<String> columnName,
                                                  List<String> dataType)
    {
        return metaClient.createTable(dbName, tblName, storageFormatName, fiberColIndex, fiberPartitioner, timestampColIndex, columnName, dataType);
    }

    @Override
    public StatusProto.ResponseStatus createTable(String dbName,
                                                  String tblName,
                                                  String userName,
                                                  String storageFormatName,
                                                  int fiberColIndex,
                                                  int timestampColIndex,
                                                  String fiberPartitioner,
                                                  List<String> columnName,
                                                  List<String> dataType)
    {
        return metaClient.createTable(dbName, tblName, userName, storageFormatName, fiberColIndex, fiberPartitioner, timestampColIndex, columnName, dataType);
    }

    @Override
    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }

    public boolean existsTopic(String topic)
    {
        try {
            Set<String> topics = kafkaAdminClient.listTopics().names().get();
            return topics.contains(topic);
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return false;
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
