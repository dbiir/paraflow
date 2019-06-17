/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTransactionHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowConnector
        implements Connector
{
    private final Logger logger = Logger.get(ParaflowConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ParaflowMetadataFactory paraflowMetadataFactory;
    private final ParaflowSplitManager paraflowSplitManager;
    private final ParaflowPageSourceProvider paraflowPageSourceProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, ParaflowMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public ParaflowConnector(
            LifeCycleManager lifeCycleManager,
            ParaflowMetadataFactory paraflowMetadataFactory,
            ParaflowSplitManager paraflowSplitManager,
            ParaflowPageSourceProvider paraflowPageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.paraflowMetadataFactory = requireNonNull(paraflowMetadataFactory, "paraflowMetadataFactory is null");
        this.paraflowSplitManager = requireNonNull(paraflowSplitManager, "paraflowSplitManager is null");
        this.paraflowPageSourceProvider = requireNonNull(paraflowPageSourceProvider, "paraflowPageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        ParaflowTransactionHandle transaction = new ParaflowTransactionHandle();
        transactions.putIfAbsent(transaction, paraflowMetadataFactory.create());
        return transaction;
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     *
     * @param transactionHandle transaction handle
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        ParaflowMetadata metadata = transactions.get(transactionHandle);
        checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
        return paraflowMetadataFactory.create();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return paraflowSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return paraflowPageSourceProvider;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * sql are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    @Override
    public void shutdown()
    {
        try {
            paraflowMetadataFactory.shutdown();
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            logger.error(e, "Error shutting down hdfs connector");
        }
    }
}
