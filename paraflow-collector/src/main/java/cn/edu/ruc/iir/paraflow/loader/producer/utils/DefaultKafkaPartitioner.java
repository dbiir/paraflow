package cn.edu.ruc.iir.paraflow.loader.producer.utils;

import org.apache.kafka.common.Cluster;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultKafkaPartitioner extends KafkaPartitioner
{
    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
    {
        int partitionNum = cluster.partitionCountForTopic(topic);
        return partitionByMod((Long) key, partitionNum);
    }

    private int partitionByMod(long key, int base)
    {
        return (int) (key % base);
    }
}
