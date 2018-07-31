package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.threads.DataPullThread;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataPullTest
{
    @Test
    public void testDataPull()
    {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TopicPartition topicPartition = new TopicPartition("test-mock", i);
            topicPartitions.add(topicPartition);
        }
        DataPullThread dataPullThread = new DataPullThread("consumer", topicPartitions);
        dataPullThread.run();
    }
}
