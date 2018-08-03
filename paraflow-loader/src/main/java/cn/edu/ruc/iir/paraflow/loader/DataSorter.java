package cn.edu.ruc.iir.paraflow.loader;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataSorter
        extends Processor
{
    private final long lifetime;
    private final int capacity;
    private final ConcurrentQueue<ParaflowRecord> pullerSorterConcurrentQueue;
    private final BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue;
    private final ParaflowRecord[][] fiberBuffers;
    private final int[] fiberIndices;
    private long lifeStart = 0L;

    public DataSorter(String name, int parallelism, long lifetime, int capacity,
                      ConcurrentQueue<ParaflowRecord> pullerSorterConcurrentQueue,
                      BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue,
                      int partitionNum)
    {
        super(name, parallelism);
        this.lifetime = lifetime;
        this.capacity = capacity;
        this.pullerSorterConcurrentQueue = pullerSorterConcurrentQueue;
        this.sorterCompactorBlockingQueue = sorterCompactorBlockingQueue;
        this.fiberBuffers = new ParaflowRecord[partitionNum][];
        this.fiberIndices = new int[partitionNum];
    }

    @Override
    public void run()
    {
        lifeStart = System.currentTimeMillis();
        while (!isReadyToStop.get()) {
            long current = System.currentTimeMillis();
            if ((current - lifeStart) >= lifetime) {
                // sort
                sort(-1);
            }
            if (!pullerSorterConcurrentQueue.isEmpty()) {
                ParaflowRecord record = pullerSorterConcurrentQueue.poll();
                int fiberId = record.getFiberId();
                if (fiberBuffers[fiberId] == null) {
                    fiberBuffers[fiberId] = new ParaflowRecord[capacity];
                }
                int index = fiberIndices[fiberId];
                if (index >= capacity) {
                    // sort
                    sort(fiberId);
                }
                fiberBuffers[fiberId][index] = record;
                fiberIndices[fiberId] = index + 1;
            }
        }
    }

    /**
     * compact the fiber buffer
     *
     * @param fiberId which fiber to be compacted. if -1, all fibers will be compacted.
     * */
    private void sort(int fiberId)
    {
        // sort all fibers
        if (fiberId == -1) {
            for (int i = 0; i < fiberBuffers.length; i++) {
                ParaflowRecord[] fiberBuffer = fiberBuffers[i];
                if (fiberBuffer == null) {
                    continue;
                }
                Arrays.sort(fiberBuffer, Comparator.comparingLong(ParaflowRecord::getTimestamp));
                ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(fiberBuffers);
                sorterCompactorBlockingQueue.offer(sortedBuffer);
                fiberIndices[i] = 0;
            }
            // reset life start time
            lifeStart = System.currentTimeMillis();
        }
        // sort the specified fiber
        else {
            if (fiberId >= fiberBuffers.length || fiberBuffers[fiberId] == null) {
                return;
            }
            ParaflowRecord[] fiberBuffer = fiberBuffers[fiberId];
            Arrays.sort(fiberBuffer, Comparator.comparingLong(ParaflowRecord::getTimestamp));
            ParaflowRecord[][] copyBuffer = new ParaflowRecord[fiberBuffers.length][];
            copyBuffer[fiberId] = fiberBuffer;
            ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(copyBuffer);
            sorterCompactorBlockingQueue.offer(sortedBuffer);
            // reset the index of the compacted fiber
            fiberIndices[fiberId] = 0;
        }
    }
}
