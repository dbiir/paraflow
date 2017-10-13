package cn.edu.ruc.iir.paraflow.loader.consumer;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.FiberFuncMapBuffer;
import cn.edu.ruc.iir.paraflow.commons.utils.FormTopicName;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageListComparator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class DefaultConsumer
{
    private final MetaClient metaClient;
    KafkaConsumer<Long, Message> consumer;
    private final FiberFuncMapBuffer funcMapBuffer = FiberFuncMapBuffer.INSTANCE();

    public DefaultConsumer(String configPath) throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(configPath);
        config.validate();
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
        consumer.assign(topicPartitions);
        while (true) {
            LinkedList<Message> messages = new LinkedList<>();
            ConsumerRecords<Long, Message> records = consumer.poll(100);
            for (ConsumerRecord<Long, Message> record : records) {
                Message message = record.value();
                messages.add(message);
            }
            Map<Integer, LinkedList<Message>> messageLists = new HashMap<Integer, LinkedList<Message>>();
            for (Message message1 : messages) {
                if (messageLists.keySet().contains(message1.getKeyIndex())) {
                    messageLists.get(message1.getKeyIndex()).add(message1);
                }
                else {
                    messageLists.put(message1.getKeyIndex(), new LinkedList<Message>());
                    messageLists.get(message1.getKeyIndex()).add(message1);
                }
            }
            for (Integer key : messageLists.keySet()) {
                Collections.sort(messageLists.get(key), new MessageListComparator());
            }
        }
    }

    public void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc)
    {
        funcMapBuffer.put(FormTopicName.formTopicName(database, table), fiberFunc);
    }

    public void shutdown()
    {
        Runtime.getRuntime().exit(0);
    }
}
