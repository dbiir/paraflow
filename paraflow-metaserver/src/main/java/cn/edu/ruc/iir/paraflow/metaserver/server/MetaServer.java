package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.MetaInitException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;
import cn.edu.ruc.iir.paraflow.metaserver.action.CreateMetaTablesAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.GetMetaTablesAction;
import cn.edu.ruc.iir.paraflow.metaserver.action.InitMetaTablesAction;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ConnectionPool;
import cn.edu.ruc.iir.paraflow.metaserver.connection.TransactionController;
import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MetaServer
{
    private static final Logger logger = LoggerFactory.getLogger(MetaServer.class);

    private Server server = null;

    private MetaServer()
    {
    }

    public static void main(String[] args)
    {
        MetaServer server = new MetaServer();
        server.start();
    }

    /**
     * Start MetaServer.
     * 1. init and validate configuration
     * 2. start rpc server
     * 3. register shutdown hook
     * 4. init connection pool
     * 5. init meta data
     */
    private void start()
    {
        StartupPipeline pipeline = new StartupPipeline();

        // add configuration init and validate hook
        pipeline.addStartupHook(
                () -> {
                    MetaConfig metaConfig = MetaConfig.INSTANCE();
                    metaConfig.init();
                }
        );

        // add rpc server start hook
        pipeline.addStartupHook(
                () -> {
                    int port = MetaConfig.INSTANCE().getServerPort();
                    ServerBuilder<NettyServerBuilder> serverBuilder =
                            NettyServerBuilder.forPort(port);
                    serverBuilder.addService(new MetaService());
                    try {
                        server = serverBuilder.build();
                        server.start();
                    }
                    catch (IOException e) {
                        throw new RPCServerIOException(port);
                    }
                    logger.info("****** RPC server started at port " + port);
                    System.out.println("****** RPC server started at port " + port);
                }
        );

        // add runtime shutdown init hook
        pipeline.addStartupHook(
                () -> Runtime.getRuntime().addShutdownHook(
                        new Thread(MetaServer.this::stop))
        );

        // add connection pool init hook
        pipeline.addStartupHook(
                () -> {
                    ConnectionPool.INSTANCE().initialize();
                    logger.info("****** Connection pool initialized successfully");
                    System.out.println("****** Connection pool initialized successfully");
                }
        );

        // add meta data init hook
        pipeline.addStartupHook(
                this::initMeta
        );

        // add server running in loop hook
        pipeline.addStartupHook(
                this::blockUntilTermination
        );

        try {
            pipeline.startUp();
        }
        catch (ParaFlowException pe) {
            pe.handle();
        }
    }

    private void initMeta() throws MetaInitException
    {
        try {
            TransactionController txController = ConnectionPool.INSTANCE().getTxController();
            txController.setAutoCommit(false);
            txController.addAction(new GetMetaTablesAction());
            txController.addAction(new CreateMetaTablesAction());
            txController.addAction(new InitMetaTablesAction());
            txController.commit();
        }
        catch (ParaFlowException e) {
            e.printStackTrace();
            throw new MetaInitException();
        }
    }

    /**
     * Stop meta server and stop all sub-modules
     * 1. Stop rpcServer
     * 2. Stop db connection
     */
    public void stop()
    {
        if (server != null) {
            logger.info("****** MetaServer shutting down...");
            System.out.println("****** MetaServer shutting down...");
            ConnectionPool.INSTANCE().close();
            server.shutdown();
            logger.info("****** MetaServer has shut down");
            System.out.println("****** MetaServer shutting down");
        }
    }

    /**
     * Await for interrupted signal to stop server
     */
    private void blockUntilTermination()
    {
        if (server != null) {
            try {
                logger.info("====== MetaServer started successfully ======");
                System.out.println("====== MetaServer started successfully ======");
                server.awaitTermination();
            }
            catch (InterruptedException e) {
                stop();
            }
        }
    }
}
