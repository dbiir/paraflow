package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;
import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;
import cn.edu.ruc.iir.paraflow.metaserver.utils.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaServer
{
    private static final Logger logger = LogManager.getLogger(MetaServer.class);

    private Server server;
    private final String metaConfigPath;

    public MetaServer(String metaConfigPath)
    {
        this.metaConfigPath = metaConfigPath;
    }

    /**
     * Start meta server.
     * 1. Start rpc server
     * 2. Connect to database
     *
     * @param port rpc server port
     * */
    public void start(int port) throws RPCServerIOException
    {
        // start gRPC server
        try {
            server = ServerBuilder.forPort(port)
                    .addService(new MetaService())
                    .build()
                    .start();
        }
        catch (IOException e) {
            throw new RPCServerIOException(port);
        }
        Runtime.getRuntime().addShutdownHook(
                new Thread(MetaServer.this::stop)
        );
        logger.info("****** RPC server started at port " + port);
        System.out.println("****** RPC server started at port " + port);

        // get config instance
        MetaConfig metaConfig = new MetaConfig(metaConfigPath);

        // connect database
        DBConnection.connect(
                metaConfig.getDBDriver(),
                metaConfig.getDBHost(),
                metaConfig.getDBUser(),
                metaConfig.getDBPassword());
        logger.info("****** Database connected successfully");
        System.out.println("****** Database connected successfully");

        logger.info("====== MetaServer started successfully ======");
        System.out.println("====== MetaServer started successfully ======");
    }

    /**
     * Stop meta server and stop all sub-modules
     * 1. Stop rpcServer
     * 2. Stop db connection
     * */
    public void stop()
    {
        if (server != null) {
            logger.info("****** MetaServer shutting down...");
            System.out.println("****** MetaServer shutting down...");
            server.shutdown();
            DBConnection.getConnectionInstance().close();
            logger.info("====== MetaServer has shut down. ======");
            System.out.println("====== MetaServer shutting down ======");
        }
    }

    /**
     * Await for interrupted signal to stop server
     * */
    public void blockUntilTermination()
    {
        if (server != null) {
            try {
                server.awaitTermination();
            }
            catch (InterruptedException e) {
                stop();
            }
        }
    }

    public static void main(String[] args)
    {
        MetaServer server = new MetaServer(args[1]);
        try {
            server.start(10012);
            server.blockUntilTermination();
        }
        catch (RPCServerIOException e) {
            e.printStackTrace();
        }
    }
}
