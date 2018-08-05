package cn.edu.ruc.iir.paraflow.commons;

/**
 * paraflow
 *
 * @author guodong
 */
public class Stats
{
    private final long interval;

    private long windowCount;
    private long windowBytes;
    private long windowStart;

    public Stats(long interval)
    {
        this.interval = interval;
        windowStart = System.currentTimeMillis();
    }

    public void record(int bytes, int count)
    {
        long time = System.currentTimeMillis();
        this.windowBytes += bytes;
        this.windowCount += count;
        if (time - windowStart >= interval) {
            printWindow(time - windowStart);
            newWindow();
        }
    }

    private void printWindow(long elapsed)
    {
        double recPerSec = 1000.0 * windowCount / (double) elapsed;
        double mbPerSec = 1000.0 * windowBytes / (double) elapsed / (1024.0 * 1024.0);
        System.out.printf("%d records processed, %.1f records/sec (%.2f MB/sec)\n", windowCount, recPerSec, mbPerSec);
    }

    private void newWindow()
    {
        windowStart = System.currentTimeMillis();
        windowCount = 0;
        windowBytes = 0;
    }
}
