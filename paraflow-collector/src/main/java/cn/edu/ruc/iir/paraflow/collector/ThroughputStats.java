package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * paraflow
 *
 * @author guodong
 */
public class ThroughputStats
{
    private static final Logger logger = LoggerFactory.getLogger(ThroughputStats.class);
    private final long interval;
    private final boolean metricEnabled;
    private final Metric metric;

    private long windowCount;
    private long windowBytes;
    private long windowStart;

    public ThroughputStats(long interval, boolean metricEnabled, String gateWayUrl, String id)
    {
        this.interval = interval;
        this.metricEnabled = metricEnabled;
        windowStart = System.currentTimeMillis();
        this.metric = new Metric(gateWayUrl, id, "collector_throughput", "Collector Throughput (MB/s)", "paraflow_collector");
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
        logger.info(windowCount + " records processed, " + recPerSec + " records/sec (" + mbPerSec + "MB/sec)");
        if (metricEnabled) {
            metric.addValue(mbPerSec);
        }
    }

    private void newWindow()
    {
        windowStart = System.currentTimeMillis();
        windowCount = 0;
        windowBytes = 0;
    }
}
