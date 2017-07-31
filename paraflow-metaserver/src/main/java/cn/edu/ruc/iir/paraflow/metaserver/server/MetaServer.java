package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaServer
{
    private static final Logger logger = Logger.getLogger(MetaServer.class.getName());

    private Server server;

    public void start(int port) throws IOException
    {
        server = ServerBuilder.forPort(port)
                .addService(new MetaService())
                .build()
                .start();
        logger.info("Server started at port " + port);

        Runtime.getRuntime().addShutdownHook(
                new Thread(MetaServer.this::stop)
        );
    }

    public void stop()
    {
        if (server != null) {
            logger.info("***Server shutting down...");
            server.shutdown();
            logger.info("***Server has shut down.");
        }
    }

    public void blockUntilTermination() throws InterruptedException
    {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args)
    {
        MetaServer server = new MetaServer();
        try {
            server.start(10012);
            server.blockUntilTermination();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
