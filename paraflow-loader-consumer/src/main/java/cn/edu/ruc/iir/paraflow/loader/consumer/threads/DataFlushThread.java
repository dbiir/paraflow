package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.FlushQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataFlushThread extends DataThread
{
    private final FlushQueueBuffer flushQueueBuffer = FlushQueueBuffer.INSTANCE();
    private final MetaClient metaClient;

    public DataFlushThread(String threadName)
    {
        super(threadName);
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
    }

    @Override
    public void run()
    {
        System.out.println(threadName + " started.");
        try {
            while (true) {
                if (isReadyToStop && flushQueueBuffer.isEmpty()) {
                    System.out.println("Thread stop");
                    return;
                }
                Optional<BufferSegment> segmentOp = flushQueueBuffer.getSegment();
                if (segmentOp.isPresent()) {
                    BufferSegment segment = segmentOp.get();
                    // flush out segment to disk
                    if (flushData(segment)) {
                        // records metadata
                        flushMeta(segment);
                    }
                    else {
                        // error dealing
                        System.out.println("Flush out failed!");
                    }
                }
                else {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println(threadName + " stopped");
        }
    }

    abstract boolean flushData(BufferSegment segment);

    private void flushMeta(BufferSegment segment)
    {
        String topic = segment.getFiberPartitions().get(0).getTopic();
        int indexOfDot = topic.indexOf(".");
        String dbName = topic.substring(0, indexOfDot);
        int length = topic.length();
        String tblName = topic.substring(indexOfDot + 1, length);
        int fiberValue;
        String path = segment.getFilePath();
        long beginTime = segment.getTimestamps()[0];
        long endTime = segment.getTimestamps()[0];
        for (long timeStamp : segment.getTimestamps()) {
            if (timeStamp < beginTime) {
                beginTime = timeStamp;
            }
            if (timeStamp > endTime) {
                beginTime = timeStamp;
            }
        }
        for (int i = 0; i < segment.getFiberPartitions().size(); i++) {
            fiberValue = segment.getFiberPartitions().get(i).getFiber();
            metaClient.createBlockIndex(dbName, tblName, fiberValue, beginTime, beginTime, path);
        }
        //else ignore
    }
}
