package cn.edu.ruc.iir.paraflow.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;

import java.util.Map;

import static cn.edu.ruc.iir.paraflow.connector.exception.ParaflowErrorCode.CONNECTOR_INIT_ERROR;
import static java.util.Objects.requireNonNull;

public class ParaflowConnectorFactory
        implements ConnectorFactory
{
    private static final Logger logger = Logger.get(ParaflowConnectorFactory.class);
    private final String name = "paraflow";

    ParaflowConnectorFactory()
    {
        logger.info("Connector " + name + " initialized.");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ParaflowHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new ParaflowModule(connectorId, context.getTypeManager())
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(ParaflowConnector.class);
        }
        catch (Exception e) {
            throw new PrestoException(CONNECTOR_INIT_ERROR, e);
        }
    }
}
