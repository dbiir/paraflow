package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.generator.TimeGenerationPool;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TimeGenerationPoolTest
{
    @Test
    public void testTimeGeneration()
    {
        TimeGenerationPool timeGenerationPool = TimeGenerationPool.INSTANCE();
        timeGenerationPool.init(System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000), 1, 80000);
        long initTime = 0;
        for (int i = 0; i < 800000; i++) {
            long time = timeGenerationPool.nextTime();
            if (time != initTime) {
                System.out.println(time);
                initTime = time;
            }
        }
    }
}
