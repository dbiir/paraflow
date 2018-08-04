package cn.edu.ruc.iir.paraflow.loader;

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
    private final int threshold;
    private final int partitionNum;
    private final BlockingQueue<ParaflowSortedBuffer> sorterCompactorBlockingQueue;
    private final ArrayList<ParaflowRecord>[] tempBuffer;
    private final SegmentContainer segmentContainer;
    private int recordNum = 0;

    public DataCompactor(String name, String db, String table, int parallelism, int threshold, int partitionNum,
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
        try {
            while (!isReadyToStop.get()) {
                ParaflowSortedBuffer sortedBuffer = sorterCompactorBlockingQueue.poll(100, TimeUnit.MILLISECONDS);
                if (sortedBuffer == null) {
                    continue;
                }
                ParaflowRecord[][] sortedRecords = sortedBuffer.getSortedRecords();
                for (int i = 0; i < sortedRecords.length; i++) {
                    if (sortedRecords[i] == null) {
                        continue;
                    }
                    ArrayList<ParaflowRecord> records = tempBuffer[i];
                    if (records == null) {
                        records = new ArrayList<>();
                        tempBuffer[i] = records;
                    }
                    records.addAll(Arrays.asList(sortedRecords[i]));
                    recordNum += sortedRecords[i].length;
                }
                if (recordNum >= threshold) {
                    // compact
                    ParaflowSegment segment = compact();
                    segment.setDb(db);
                    segment.setTable(table);
                    segmentContainer.addSegment(segment);
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ParaflowSegment compact()
    {
        ParaflowRecord[][] compactedRecords = new ParaflowRecord[partitionNum][];
        for (int i = 0; i < partitionNum; i++) {
            tempBuffer[i].sort(Comparator.comparingLong(ParaflowRecord::getTimestamp));
            tempBuffer[i].toArray(compactedRecords[i]);
        }
        recordNum = 0;
        return new ParaflowSegment(compactedRecords);
    }
}
