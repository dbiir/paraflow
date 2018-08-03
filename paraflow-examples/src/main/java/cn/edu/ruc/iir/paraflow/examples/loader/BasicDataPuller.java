package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.DefaultLoader;
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
    {
        String topicName = "test-0802";
        int partitionNum = 10;

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitionNum; i++) {
            topicPartitions.add(new TopicPartition(topicName, i));
        }
        try {
            DefaultLoader loader = new DefaultLoader(args[0]);
            loader.consume(topicPartitions, new MockTableTransformer(),
                    Runtime.getRuntime().availableProcessors() * 2,
                    5000, 500000, 500000, 5000);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
