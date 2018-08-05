package cn.edu.ruc.iir.paraflow.loader;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowSortedBuffer
{
    private final ParaflowRecord[][] sortedRecords;

    public ParaflowSortedBuffer(ParaflowRecord[][] sortedRecords)
    {
        this.sortedRecords = sortedRecords;
    }

    public ParaflowRecord[][] getSortedRecords()
    {
        return sortedRecords;
    }
}
