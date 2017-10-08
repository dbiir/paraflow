package cn.edu.ruc.iir.paraflow.loader.producer;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.producer.buffer.BlockingQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.buffer.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.loader.producer.threads.ThreadManager;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.ProducerConfig;
import cn.edu.ruc.iir.paraflow.loader.producer.utils.Utils;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultProducer implements Producer
{
    private final MetaClient metaClient;
    private final AdminClient kafkaAdminClient;
    private final BlockingQueueBuffer buffer = BlockingQueueBuffer.INSTANCE();
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private final long offerTimeout;

    public DefaultProducer(String configPath) throws ConfigFileNotFoundException
    {
        ProducerConfig config = ProducerConfig.INSTANCE();
            config.init(configPath);
            config.validate();
        this.offerTimeout = config.getBufferOfferTimeout();
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
        // todo init meta cache
        ThreadManager.INSTANCE().init();
        // register shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(ThreadManager.INSTANCE()::shutdown)
        );
        ThreadManager.INSTANCE().run();
    }

    @Override
    public void send(String database, String table, Message message)
    {
        message.setTopic(Utils.formTopicName(database, table));
        while (true) {
            try {
                buffer.offer(message, offerTimeout);
                break;
            }
            catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public void createTopic(String topicName, int partitionsNum, short replicationFactor)
    {
        NewTopic newTopic = new NewTopic(topicName, partitionsNum, replicationFactor);
        CreateTopicsResult result = kafkaAdminClient.createTopics(Collections.singletonList(newTopic));
        KafkaFuture future = result.values().get(topicName);
        try
        {
            future.get();
        } catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
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
    public void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc)
    {
        funcMapBuffer.put(Utils.formTopicName(database, table), fiberFunc);
    }

//    @Override
//    public StatusProto.ResponseStatus createFiberFunc(String funcName, SerializableFunction<String, Long> func) throws IOException
//    {
////        return metaClient.createFiberFunc(funcName, func);
//        return StatusProto.ResponseStatus.newBuilder().build();
//    }

    @Override
    public void registerFilter(String database, String table, Function<Message, Boolean> filterFunc)
    {
        // todo register filters currently not supported
    }

    @Override
    public void registerTransformer(String database, String table, Function<Message, Message> transformerFunc)
    {
        // todo register transformer currently not supported
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
            metaClient.shutdown(ProducerConfig.INSTANCE().getMetaClientShutdownTimeout());
        }
        catch (InterruptedException e) {
            metaClient.shutdownNow();
        }
    }
}
