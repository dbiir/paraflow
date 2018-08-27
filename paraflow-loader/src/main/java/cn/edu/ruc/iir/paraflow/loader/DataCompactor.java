package cn.edu.ruc.iir.paraflow.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class DataCompactor
        extends Processor
{
    private static final Logger logger = LoggerFactory.getLogger(DataCompactor.class);
    private final int threshold;
    private final int partitionNum;
    private final BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue;
    private final ArrayList<ParaflowRecord>[] tempBuffer;
    private final SegmentContainer segmentContainer;
    private int recordNum = 0;

    DataCompactor(String name, String db, String table, int parallelism, int threshold, int partitionNum,
                  BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue)
    {
        super(name, db, table, parallelism);
        this.threshold = threshold;
        this.partitionNum = partitionNum;
        this.sorterCompactorBlockingQueue = sorterCompactorBlockingQueue;
        this.tempBuffer = new ArrayList[partitionNum];
        this.segmentContainer = SegmentContainer.INSTANCE();
    }

    @Override
    public void run()
    {
        System.out.println(super.name + " started.");
        logger.info(super.name + " started.");
        try {
            while (!isReadyToStop.get()) {
                ParaflowSortedBuffer sortedBuffer
                        = sorterCompactorBlockingQueue.poll(100, TimeUnit.MILLISECONDS);
                if (sortedBuffer == null) {
                    continue;
                }
                System.out.println("compactor gets sorted buffer.");
                logger.debug("compactor gets sorted buffer.");
                ParaflowRecord[] sortedRecords = sortedBuffer.getSortedRecords();
                int partition = sortedBuffer.getPartition();
                if (tempBuffer[partition] == null) {
                    tempBuffer[partition] = new ArrayList<>();
                }
                tempBuffer[partition].addAll(Arrays.asList(sortedRecords));
                recordNum += sortedRecords.length;
                if (recordNum >= threshold) {
                    // compact
                    ParaflowSegment segment = compact();
                    segment.setDb(db);
                    segment.setTable(table);
                    while (!segmentContainer.addSegment(segment)) {
                        Thread.yield();
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ParaflowSegment compact()
    {
        System.out.println("compacting....");
        logger.debug("compacting....");
        ParaflowRecord[] compactedRecords = new ParaflowRecord[recordNum];
        long[] fiberMinTimestamps = new long[partitionNum];
        long[] fiberMaxTimestamps = new long[partitionNum];
        int compactedIndex = 0;
        for (int i = 0; i < partitionNum; i++) {
            if (tempBuffer[i] != null && !tempBuffer[i].isEmpty()) {
                tempBuffer[i].sort(Comparator.comparingLong(ParaflowRecord::getTimestamp));
                int tempSize = tempBuffer[i].size();
                ParaflowRecord[] tempRecords = new ParaflowRecord[tempSize];
                tempBuffer[i].toArray(tempRecords);
                fiberMinTimestamps[i] = tempRecords[0].getTimestamp();
                fiberMaxTimestamps[i] = tempRecords[tempRecords.length - 1].getTimestamp();
                System.arraycopy(tempRecords, 0, compactedRecords, compactedIndex, tempSize);
                compactedIndex += tempSize;
                tempBuffer[i].clear();
            }
            else {
                fiberMinTimestamps[i] = -1;
                fiberMaxTimestamps[i] = -1;
            }
        }
        recordNum = 0;
        return new ParaflowSegment(compactedRecords, fiberMinTimestamps, fiberMaxTimestamps);
    }
}
