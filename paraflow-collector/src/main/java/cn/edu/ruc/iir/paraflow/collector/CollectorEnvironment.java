package cn.edu.ruc.iir.paraflow.collector;

/**
 * collector environment
 *
 * @author guodong
 */
public class CollectorEnvironment
{
    private CollectorEnvironment()
    {
    }

    public static CollectorEnvironment getEnvironment()
    {
        return new CollectorEnvironment();
    }

    public <T> DataFlow<T> addSource(DataSource source)
    {
        return new FiberFlow<>(source.getName(), source);
    }
}
