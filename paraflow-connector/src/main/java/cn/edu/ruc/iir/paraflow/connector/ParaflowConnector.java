package cn.edu.ruc.iir.paraflow.connector;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final ParaflowMetadataFactory paraflowMetadataFactory;
    private final ParaflowSplitManager paraflowSplitManager;
    private final ParaflowPageSourceProvider paraflowPageSourceProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, ParaflowMetadata> transactions =
            new ConcurrentHashMap<>();

    @Inject
    public ParaflowConnector(
                    LifeCycleManager lifeCycleManager,
                    ParaflowMetadataFactory paraflowMetadataFactory,
                    ParaflowSplitManager paraflowSplitManager,
                    ParaflowPageSourceProvider paraflowPageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager lis null");
        this.paraflowMetadataFactory = requireNonNull(paraflowMetadataFactory, "paraflowMetadataFactory is null");
        this.paraflowSplitManager = requireNonNull(paraflowSplitManager, "paraflowSplitManager is null");
        this.paraflowPageSourceProvider = requireNonNull(paraflowPageSourceProvider, "paraflowPageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return null;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return null;
    }
}
