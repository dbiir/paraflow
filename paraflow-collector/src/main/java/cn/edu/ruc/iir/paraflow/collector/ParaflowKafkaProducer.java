package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.collector.utils.CollectorConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowKafkaProducer
{
    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final AtomicLong ackRecords = new AtomicLong();
    private final ThroughputStats throughputStats;

    public ParaflowKafkaProducer(CollectorConfig conf, long statsInterval)
    {
        Properties config = conf.getProperties();
        // set the producer configuration properties for kafka record key and value serializers
        if (!config.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        }
        if (!config.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        }
        if (!config.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be specified in the config");
        }
        kafkaProducer = new KafkaProducer<>(config);
        this.throughputStats = new ThroughputStats(statsInterval, conf.isMetricEnabled(), conf.getPushGateWayUrl(),
                conf.getCollectorId());
    }

    public void sendMsg(ProducerRecord<byte[], byte[]> record, int length)
    {
        kafkaProducer.send(record, new ProducerCallback(length, throughputStats));
    }

    public long getAckRecords()
    {
        return ackRecords.get();
    }

    public void close()
    {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private final class ProducerCallback
            implements Callback
    {
        private final int bytes;
        private final ThroughputStats throughputStats;

        ProducerCallback(int bytes, ThroughputStats throughputStats)
        {
            this.bytes = bytes;
            this.throughputStats = throughputStats;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         *                  Possible thrown exceptions include:
         *                  <p>
         *                  Non-Retriable exceptions (fatal, the message will never be sent):
         *                  <p>
         *                  InvalidTopicException
         *                  OffsetMetadataTooLargeException
         *                  RecordBatchTooLargeException
         *                  RecordTooLargeException
         *                  UnknownServerException
         *                  <p>
         *                  Retriable exceptions (transient, may be covered by increasing #.retries):
         *                  <p>
         *                  CorruptRecordException
         *                  InvalidMetadataException
         *                  NotEnoughReplicasAfterAppendException
         *                  NotEnoughReplicasException
         *                  OffsetOutOfRangeException
         *                  TimeoutException
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception)
        {
            if (exception == null) {
                this.throughputStats.record(bytes, 1);
            }
        }
    }
}
