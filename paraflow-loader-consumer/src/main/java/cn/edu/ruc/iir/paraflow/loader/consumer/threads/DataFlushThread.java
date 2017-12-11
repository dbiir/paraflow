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
    protected final MetaClient metaClient;

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
                        System.out.println("Done flushing metadata");
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
        long beginTime;
        long endTime;
        for (int i = 0; i < segment.getFiberPartitions().size(); i++) {
            beginTime = segment.getTimestamps()[2 * i];
            endTime = segment.getTimestamps()[2 * i + 1];
            fiberValue = segment.getFiberPartitions().get(i).getFiber();
            System.out.println("Index: [" + dbName + "," + tblName + "," + fiberValue + "," + beginTime + "," + endTime + "," + path + "]");
            metaClient.createBlockIndex(dbName, tblName, fiberValue, beginTime, endTime, path);
        }
        //else ignore
    }
}
