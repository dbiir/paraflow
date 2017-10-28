package cn.edu.ruc.iir.paraflow.loader.consumer;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageListComparator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
    KafkaConsumer<Long, Message> consumer;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();
//    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private final long offerBlockSize;
    private String hdfsWarehouse;
    private String dbName;
    private String tblName;
    LinkedList<Message> messages = new LinkedList<>();
    Map<Integer, LinkedList<Message>> messageLists = new HashMap<Integer, LinkedList<Message>>();

    public DefaultConsumer(String configPath) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
        this.offerBlockSize = config.getBufferOfferBlockSize();
        this.hdfsWarehouse = config.getHDFSWarehouse();
        // init meta client
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        props.setProperty("group.id", config.getGroupId());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("key.deserializer", config.getKafkaKeyDeserializerClass());
        props.setProperty("value.deserializer", config.getKafkaValueDeserializerClass());
        consumer = new KafkaConsumer<>(props);
        //init();
    }

//    private void init()
//    {
//        // todo init meta cache
//        ThreadManager.INSTANCE().init();
//        // register shutdown hook
//        Runtime.getRuntime().addShutdownHook(
//                new Thread(this::beforeShutdown)
//        );
//        Runtime.getRuntime().addShutdownHook(
//                new Thread(ThreadManager.INSTANCE()::shutdown)
//        );
//        ThreadManager.INSTANCE().run();
//    }

    public void consume(LinkedList<TopicPartition> topicPartitions)
    {
        int count;
        String topic = topicPartitions.get(0).topic();
        int indexofdot = topic.indexOf(".");
        int length = topic.length();
        dbName = topic.substring(0, indexofdot);
        tblName = topic.substring(indexofdot + 1, length);
        consumer.assign(topicPartitions);
        while (true) {
            ConsumerRecords<Long, Message> records = consumer.poll(1000);
            System.out.println("records count: " + records.count());
            int i = 0;
            for (ConsumerRecord<Long, Message> record : records) {
                Message message = record.value();
//                System.out.println("i : " + i++);
//                System.out.println("message : " + message);
//                System.out.println("msg key: " + message.getKey());
//                if (buffer.offer(message)) {
                    messages.add(message);
//                }
//                else {
//                    commit(record.offset(), record.topic(), record.partition());
//                    break;
//                }
            }
            count = messages.size();
            System.out.println("message size : " + count);
            sort(messages);
            for (Integer key : messageLists.keySet()) {
                for (i = 0; i < count; i++) {
                    System.out.println("messagesLists.get(key).get(i) : " + messageLists.get(key).get(i));
                }
            }
//            String fileName = String.format("%s%s",topicPartitions.get(0).topic(), timeStamp);
            flush(messageLists);
        }
    }

    public void sort(LinkedList<Message> messages)
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

    public void commit(Long offset, String topic, int partition)
    {
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Map<TopicPartition, OffsetAndMetadata> offsetParam
                = new HashMap<TopicPartition, OffsetAndMetadata>();
        offsetParam.put(topicPartition, offsetAndMetadata);
        consumer.commitSync(offsetParam);
    }

    public void flush(Map<Integer, LinkedList<Message>> messageLists)
    {
        System.out.println("hdfsWarehouse : " + hdfsWarehouse);
        System.out.println("dbName : " + dbName);
        System.out.println("tblName : " + tblName);
        String file = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
        System.out.println("file : " + file);
        Path path = new Path(file);
        Configuration conf = new Configuration();
        FileSystem fs;
        FSDataOutputStream output = null;
        try {
            fs = path.getFileSystem(conf);
            //fs = FileSystem.get(conf);
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
//        finally {
//            try {
//                output.close();
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }

    public void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc)
    {
        funcMapBuffer.put(FormTopicName.formTopicName(database, table), fiberFunc);
    }

    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }

    public void clear()
    {
        messages.clear();
        messageLists.clear();
    }
}
