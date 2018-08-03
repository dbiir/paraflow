package cn.edu.ruc.iir.paraflow.loader;

/**
 * paraflow segment
 *
 * @author guodong
 */
public class ParaflowSegment
{
    public enum StorageLevel {
        IN_HEAP, OFF_HEAP, ON_DISK
    }

    private final ParaflowRecord[][] records;   // records of each fiber

    public ParaflowSegment(ParaflowRecord[][] records)
    {
        this.records = records;
    }

    public ParaflowRecord[][] getRecords()
    {
        return records;
    }
}
