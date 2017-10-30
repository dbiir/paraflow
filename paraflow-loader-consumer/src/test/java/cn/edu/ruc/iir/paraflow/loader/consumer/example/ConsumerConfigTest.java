package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;

public class ConsumerConfigTest
{
    ConsumerConfig consumerConfig = ConsumerConfig.INSTANCE();
    public void init() throws ConfigFileNotFoundException
    {
        consumerConfig.init("/home/yixuanwang/Documents/paraflow/dist/conf/loader-consumer.conf");
    }
    public void ConsumerConfigTest()
    {
        System.out.println("consumer.thread.num : " + consumerConfig.getKafkaThreadNum());
    }

    public void getKafkaPartitionerClass()
    {
        System.out.println("kafka.partitioner : " + consumerConfig.getKafkaPartitionerClass());
    }

    public static void main(String[] args) throws ConfigFileNotFoundException
    {
        ConsumerConfigTest consumer = new ConsumerConfigTest();
        consumer.init();
        consumer.ConsumerConfigTest();
        consumer.getKafkaPartitionerClass();
    }
}
