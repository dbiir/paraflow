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

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run()
    {
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
        }
    }

    abstract boolean flushData(BufferSegment segment);

    private void flushMeta(BufferSegment segment)
    {}
}
