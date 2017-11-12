package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.FlushQueueBuffer;

import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class DataFlushThread extends DataThread
{
    private final FlushQueueBuffer flushQueueBuffer = FlushQueueBuffer.INSTANCE();

    public DataFlushThread(String threadName)
    {
        super(threadName);
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
    {}
}
