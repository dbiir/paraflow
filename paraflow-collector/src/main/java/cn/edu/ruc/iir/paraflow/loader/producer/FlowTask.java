package cn.edu.ruc.iir.paraflow.loader.producer;

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateFuture;

/**
 * paraflow
 *
 * @author guodong
 */
public class FlowTask<T>
{
    private final DataFlow<T> dataFlow;

    public FlowTask(DataFlow<T> dataFlow)
    {
        this.dataFlow = dataFlow;
    }

    public ListenableFuture<?> execute()
    {
        return immediateFuture(null);
    }
}
