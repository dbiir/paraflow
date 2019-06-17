package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;

public class DataSorter
        extends Processor
{
    private final long lifetime;
    private final int capacity;
    private final BlockingQueue<ParaflowRecord> pullerSorterConcurrentQueue;
    private final BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue;
    private final ParaflowRecord[][] fiberBuffers;
    private final int[] fiberIndices;
    private long lifeStart = 0L;

    DataSorter(String name, String db, String table, int parallelism, long lifetime, int capacity,
               BlockingQueue<ParaflowRecord> pullerSorterConcurrentQueue,
               BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue,
               int partitionNum)
    {
        super(name, db, table, parallelism);
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
        System.out.println(super.name + " started.");
        lifeStart = System.currentTimeMillis();
        while (!isReadyToStop.get()) {
            long current = System.currentTimeMillis();
            System.out.println("Current time: " + current);
            if ((current - lifeStart) >= lifetime) {
                System.out.println("Sorting -1");
                sort(-1);
            }
            try {
                System.out.println("Sorter taking record.");
                ParaflowRecord record = pullerSorterConcurrentQueue.take();
                System.out.println("Sorter taken record.");
                int fiberId = record.getFiberId();
                if (fiberBuffers[fiberId] == null) {
                    fiberBuffers[fiberId] = new ParaflowRecord[capacity];
                }
                int index = fiberIndices[fiberId];
                System.out.println("Checking sorting condition.");
                if (index >= capacity) {
                    // sort
                    System.out.println("Sorting " + fiberId);
                    sort(fiberId);
                }
                System.out.println("Updating record.");
                fiberBuffers[fiberId][index] = record;
                fiberIndices[fiberId] = index + 1;
            }
            catch (InterruptedException e) {
                System.out.println("Sorter interrupt");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * compact the fiber buffer
     *
     * @param fiberId which fiber to be compacted. if -1, all fibers will be compacted.
     */
    private void sort(int fiberId)
    {
        // sort all fibers
        if (fiberId == -1) {
//            Arrays.stream(fiberBuffers).forEach(fiberBuffer -> {
//                if (fiberBuffer != null) {
//                    Arrays.sort(fiberBuffer, Comparator.comparingLong(ParaflowRecord::getTimestamp));
//                    ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(fiberBuffers);
////                    sorterCompactorBlockingQueue.offer(sortedBuffer);
//                    System.out.println("sorted buffer appended");
//                }
//            });
            for (int i = 0; i < fiberBuffers.length; i++) {
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
//            ParaflowRecord[][] copyBuffer = new ParaflowRecord[fiberBuffers.length][];
//            copyBuffer[fiberId] = fiberBuffer;
//            ParaflowSortedBuffer sortedBuffer = new ParaflowSortedBuffer(copyBuffer);
//            sorterCompactorBlockingQueue.offer(sortedBuffer);
            // reset the index of the compacted fiber
            fiberIndices[fiberId] = 0;
            System.out.println("sorted buffer appended, queue size: " + pullerSorterConcurrentQueue.remainingCapacity());
        }
    }
}
