package cn.edu.ruc.iir.paraflow.metaserver.action;

/**
 * paraflow
 *
 * @author guodong
 */
public class ActionResponse<T>
{
    private final T response;

    public ActionResponse(T response)
    {
        this.response = response;
    }

    public T getResponse()
    {
        return response;
    }
}
