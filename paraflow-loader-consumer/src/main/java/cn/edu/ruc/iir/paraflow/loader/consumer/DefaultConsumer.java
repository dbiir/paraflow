package cn.edu.ruc.iir.paraflow.loader.consumer;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.threads.ConsumerThreadManager;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageListComparator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class DefaultConsumer implements Consumer
{
    private final MetaClient metaClient;
    private final AdminClient kafkaAdminClient;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
    private String hdfsWarehouse;
    private String dbName;
    private String tblName;
    private LinkedList<Message> messages = new LinkedList<>();
    private Map<Integer, LinkedList<Message>> messageLists = new HashMap<>();
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private String topic;
    private ConsumerThreadManager consumerThreadManager;

    public DefaultConsumer(String configPath, LinkedList<TopicPartition> topicPartitions) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
        this.topic = topicPartitions.get(0).topic();
        this.hdfsWarehouse = config.getHDFSWarehouse();
        // init meta client
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
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
        consumerThreadManager = ConsumerThreadManager.INSTANCE();
        init(topicPartitions);
    }

    private void init(LinkedList<TopicPartition> topicPartitions)
    {
        // todo init meta cache
        consumerThreadManager.init(topicPartitions);
        // register shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::beforeShutdown)
        );
        Runtime.getRuntime().addShutdownHook(
                new Thread(ConsumerThreadManager.INSTANCE()::shutdown)
        );
        consumerThreadManager.run();
    }

    public void consume()
    {
        int count;
        while (true) {
            if (buffer.isEmpty()) {
                System.out.println("Thread stop");
                return;
            }
            count = buffer.drainTo(messages);
            int indexOfDot = topic.indexOf(".");
            int length = topic.length();
            this.dbName = topic.substring(0, indexOfDot - 1);
            this.tblName = topic.substring(indexOfDot + 1, length - 1);
            System.out.println("message count : " + count);
            sort(messages);
//            for (Integer key : messageLists.keySet()) {
//                for (int i = 0; i < count; i++) {
//                    System.out.println("messagesLists.get(key).get(i) : " + messageLists.get(key).get(i));
//                }
//            }
            flush(messageLists);
            writeToMetaData();
        }
    }

    private void sort(LinkedList<Message> messages)
    {
        for (Message message1 : messages) {
            if (messageLists.keySet().contains(message1.getKeyIndex())) {
                messageLists.get(message1.getKeyIndex()).add(message1);
            }
            else {
                messageLists.put(message1.getKeyIndex(), new LinkedList<Message>());
                messageLists.get(message1.getKeyIndex()).add(message1);
            }
        }
        //sort in every messageList
        for (Integer key : messageLists.keySet()) {
            Collections.sort(messageLists.get(key), new MessageListComparator());
        }
    }

    private void flush(Map<Integer, LinkedList<Message>> messageLists)
    {
        System.out.println("dbName : " + dbName);
        System.out.println("tblName : " + tblName);
        String file = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
        System.out.println("file : " + file);
        Path path = new Path(file);
        Configuration conf = new Configuration();
        FileSystem fs;
        FSDataOutputStream output;
        try {
            fs = path.getFileSystem(conf);
            output = fs.create(path);
            for (Integer key : messageLists.keySet()) {
                for (Message message : messageLists.get(key)) {
                    String result = org.apache.commons.lang.StringUtils.join(message.getValues());
                    result += message.getTimestamp();
                    output.write(result.getBytes("UTF-8"));
                }
            }
            output.flush();
            output.close();
            fs.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToMetaData()
    {
        int fiberValue;
        long timeBegin;
        long timeEnd;
        String path;
        if (messages.get(0).getTimestamp().isPresent()) {
            timeBegin = messages.get(0).getTimestamp().get();
            timeEnd = messages.get(0).getTimestamp().get();
            for (Integer key : messageLists.keySet()) {
                if (key < timeBegin) {
                    timeBegin = key;
                }
                if (key > timeEnd) {
                    timeEnd = key;
                }
            }
            fiberValue = Integer.parseInt(messages.get(0).getValues()[messages.get(0).getKeyIndex()]);
            path = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
            metaClient.createBlockIndex(dbName, tblName, fiberValue, timeBegin, timeEnd, path);
        }
        //else ignore
    }

    public void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc)
    {
        funcMapBuffer.put(FormTopicName.formTopicName(database, table), fiberFunc);
    }

    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }

    private void beforeShutdown()
    {
        messages.clear();
        messageLists.clear();
        kafkaAdminClient.close();
        try {
            metaClient.shutdown(ConsumerConfig.INSTANCE().getMetaClientShutdownTimeout());
        }
        catch (InterruptedException e) {
            metaClient.shutdownNow();
        }
    }
}
