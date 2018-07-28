package cn.edu.ruc.iir.paraflow.collector;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Properties;

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
    private long startTime;

    public FlowTask(DataFlow<T> dataFlow, Properties conf)
    {
        this.dataFlow = dataFlow;
        String topic = dataFlow.getSink().getDb() + "-" + dataFlow.getSink().getTbl();
        this.kafkaProducer = new ParaflowKafkaProducer(topic, dataFlow.getPartitioner(), conf);
    }

    public ListenableFuture<?> execute()
    {
        startTime = System.currentTimeMillis();
        while (!dataFlow.isFinished()) {
            kafkaProducer.sendMsg(dataFlow.next());
        }
        return immediateFuture(null);
    }

    public double flowSpeed()
    {
        return kafkaProducer.getAckRecords() / (System.currentTimeMillis() - startTime);
    }

    public void close()
    {
        kafkaProducer.close();
    }
}
