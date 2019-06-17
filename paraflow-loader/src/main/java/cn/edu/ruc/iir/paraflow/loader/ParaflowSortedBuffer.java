package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;

public class ParaflowSortedBuffer
{
    private final ParaflowRecord[] sortedRecords;
    private final int partition;

    public ParaflowSortedBuffer(ParaflowRecord[] sortedRecords, int partition)
    {
        this.sortedRecords = sortedRecords;
        this.partition = partition;
    }

    public ParaflowRecord[] getSortedRecords()
    {
        return sortedRecords;
    }

    public int getPartition()
    {
        return partition;
    }
}
