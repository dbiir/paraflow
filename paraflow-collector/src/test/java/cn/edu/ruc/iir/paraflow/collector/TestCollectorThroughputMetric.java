package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.collector.utils.CollectorThroughputMetric;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestCollectorThroughputMetric
{
    @Test
    public void testMetric()
            throws InterruptedException
    {
        CollectorThroughputMetric metric = new CollectorThroughputMetric("collector0", "dbiir00:9101");
        for (int i = 0; i < 10; i++) {
            metric.addValue(1000.0 * i);
            Thread.sleep(2000);
        }
    }
}
