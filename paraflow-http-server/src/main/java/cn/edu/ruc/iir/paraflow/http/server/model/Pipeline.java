package cn.edu.ruc.iir.paraflow.http.server.model;

/**
 * paraflow
 *
 * @author guodong
 */
public class Pipeline
{
    private final String name;
    private float speed = 100.1f;

    public Pipeline(
            String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public float getSpeed()
    {
        return speed;
    }
}
