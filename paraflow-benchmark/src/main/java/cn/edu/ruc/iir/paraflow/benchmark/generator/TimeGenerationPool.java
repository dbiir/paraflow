package cn.edu.ruc.iir.paraflow.benchmark.generator;

/**
 * TimeGenerationPool generates timestamps based on user-defined frequency.
 * stride: how long we calculate the frequency.
 * frequency * stride: the number of records for every stride(like 1, 5, 10, etc.) seconds.
 *
 * @author guodong
 */
public class TimeGenerationPool
{
    private long currentTime;
    private long frequency;
    private long stride;
    private long counter = 0;

    private TimeGenerationPool()
    {
    }

    public static final TimeGenerationPool INSTANCE()
    {
        return TimeGenerationPoolHolder.instance;
    }

    /**
     * Generation frequency. records/s
     */
    public void init(long startTime, long stride, long frequency)
    {
        this.currentTime = startTime;
        this.stride = stride;
        this.frequency = frequency;
    }

    public synchronized long nextTime()
    {
        counter++;
        if (counter > frequency) {
            currentTime++;
            counter = 0;
            return currentTime;
        }
        return currentTime;
    }

    private static final class TimeGenerationPoolHolder
    {
        private static final TimeGenerationPool instance = new TimeGenerationPool();
    }
}
