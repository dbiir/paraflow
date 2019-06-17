package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.google.common.util.concurrent.Futures.immediateFuture;

/**
 * paraflow
 *
 * @author guodong
 */
public class FlowTask<T>
{
    private final DataFlow<T> dataFlow;
    private final ParaflowKafkaProducer kafkaProducer;
    private final String topic;
    private long startTime;

    FlowTask(DataFlow<T> dataFlow, ParaflowKafkaProducer kafkaProducer)
    {
        this.dataFlow = dataFlow;
        this.topic = dataFlow.getSink().getDb() + "-" + dataFlow.getSink().getTbl();
        this.kafkaProducer = kafkaProducer;
    }

    ListenableFuture<?> execute()
    {
        startTime = System.currentTimeMillis();
        ParaflowFiberPartitioner partitioner = dataFlow.getPartitioner();
        while (!dataFlow.isFinished()) {
            Message message = dataFlow.next();
            ProducerRecord<byte[], byte[]> record;
            int partition = partitioner.getFiberId(message.getKey());
            record = new ProducerRecord<>(topic, partition, message.getTimestamp(),
                    new byte[0], message.getValue());
            kafkaProducer.sendMsg(record, message.getValue().length);
        }
        return immediateFuture(null);
    }

    public double flowSpeed()
    {
        return 1.0 * kafkaProducer.getAckRecords() / (System.currentTimeMillis() - startTime);
    }

    void close()
    {
        kafkaProducer.close();
    }
}
