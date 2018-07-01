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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.CONNECTOR_SHUTDOWN_ERROR;
import static java.util.Objects.requireNonNull;

public class ParaflowConnector
        implements Connector
{
    private final Logger logger = Logger.get(ParaflowConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ParaflowMetadata paraflowMetadata;
    private final ParaflowSplitManager paraflowSplitManager;
    private final ParaflowPageSourceProvider paraflowPageSourceProvider;

    @Inject
    public ParaflowConnector(
            LifeCycleManager lifeCycleManager,
            ParaflowMetadata paraflowMetadata,
            ParaflowSplitManager paraflowSplitManager,
            ParaflowPageSourceProvider paraflowPageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.paraflowMetadata = requireNonNull(paraflowMetadata, "paraflowMetadata is null");
        this.paraflowSplitManager = requireNonNull(paraflowSplitManager, "paraflowSplitManager is null");
        this.paraflowPageSourceProvider = requireNonNull(paraflowPageSourceProvider, "paraflowPageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return ParaflowTransactionHandle.INSTANCE;
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
        return paraflowMetadata;
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

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    @Override
    public void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            logger.error(e, "Error shutting down connector");
            throw new PrestoException(CONNECTOR_SHUTDOWN_ERROR, e);
        }
    }
}
