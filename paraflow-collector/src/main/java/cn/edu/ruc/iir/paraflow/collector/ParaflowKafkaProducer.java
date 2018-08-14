package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import cn.edu.ruc.iir.paraflow.commons.Stats;
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
    private final String topic;
    private final ParaflowFiberPartitioner partitioner;
    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final AtomicLong ackRecords = new AtomicLong();
    private final Stats stats;

    public ParaflowKafkaProducer(String topic, ParaflowFiberPartitioner partitioner,
                                 Properties config)
    {
        this.topic = topic;
        this.partitioner = partitioner;

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
        this.stats = new Stats(3000);
    }

    public void sendMsg(Message message)
    {
        ProducerRecord<byte[], byte[]> record;
        int partition = partitioner.getFiberId(message.getKey());
        record = new ProducerRecord<>(topic, partition, message.getTimestamp(),
                new byte[0], message.getValue());
        kafkaProducer.send(record, new ProducerCallback(message.getValue().length, stats));
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
        private final Stats stats;

        ProducerCallback(int bytes, Stats stats)
        {
            this.bytes = bytes;
            this.stats = stats;
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
                this.stats.record(bytes, 1);
            }
        }
    }
}
