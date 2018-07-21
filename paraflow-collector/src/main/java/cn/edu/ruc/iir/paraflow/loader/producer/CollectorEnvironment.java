package cn.edu.ruc.iir.paraflow.loader.producer;

/**
 * collector environment
 *
 * @author guodong
 */
public class CollectorEnvironment
{
    private CollectorEnvironment()
    {}

    public static CollectorEnvironment getEnvironment()
    {
        return new CollectorEnvironment();
    }

    public <T> DataFlow<T> addSource(DataSource<T> source)
    {
        return null;
    }
}
