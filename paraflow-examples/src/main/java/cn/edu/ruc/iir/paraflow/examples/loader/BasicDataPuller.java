package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.DataPuller;
import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class BasicDataPuller
{
    private BasicDataPuller()
    {}

    public static void main(String[] args)
            throws ConfigFileNotFoundException
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(args[0]);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TopicPartition topicPartition = new TopicPartition("test-0802", i);
            topicPartitions.add(topicPartition);
        }
        DataPuller dataPuller = new DataPuller("consumer", topicPartitions, config.getProperties());
        dataPuller.run();
    }
}
