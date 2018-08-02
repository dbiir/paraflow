package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.DataPuller;
import cn.edu.ruc.iir.paraflow.loader.ProcessPipeline;
import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String topicName = "test-0802";
        int partitionNum = 10;
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        config.init(args[0]);
        ProcessPipeline pipeline = new ProcessPipeline();
        int parallelism = Runtime.getRuntime().availableProcessors() * 2;
//        int parallelism = 1;
        Map<Integer, List<TopicPartition>> partitionMapping = new HashMap<>();
        for (int i = 0; i < partitionNum; i++) {
            int idx = i % parallelism;
            if (!partitionMapping.containsKey(idx)) {
                partitionMapping.put(idx, new ArrayList<>());
            }
            partitionMapping.get(idx).add(new TopicPartition(topicName, i));
        }
        for (int i = 0; i < parallelism; i++) {
            List<TopicPartition> topicPartitions = partitionMapping.get(i);
            DataPuller dataPuller = new DataPuller("consumer-" + i, 1,
                    topicPartitions, config.getProperties(), new MockTableTransformer());
            pipeline.addProcessor(dataPuller);
        }
        pipeline.start();
    }
}
