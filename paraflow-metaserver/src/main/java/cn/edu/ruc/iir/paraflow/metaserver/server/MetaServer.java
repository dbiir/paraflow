package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ConnectionPool;
import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
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

    private final String metaConfigPath;
    private Server server = null;

    private MetaServer(String metaConfigPath)
    {
        this.metaConfigPath = metaConfigPath;
    }

    /**
     * Start MetaServer.
     * 1. init and validate configuration
     * 2. start rpc server
     * 3. register shutdown hook
     * 4. init connection pool
     * 5. init meta data
     * */
    private void start()
    {
        StartupPipeline pipeline = new StartupPipeline();

        // add configuration init and validate hook
        pipeline.addStartupHook(
                () -> {
                    MetaConfig metaConfig = MetaConfig.INSTANCE();
                    metaConfig.init(metaConfigPath);
                    metaConfig.validate();
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
                this::initMetadata
        );

        // add server running in loop hook
        pipeline.addStartupHook(
                this::blockUntilTermination
        );

        try {
            pipeline.startUp();
            logger.info("====== MetaServer started successfully ======");
            System.out.println("====== MetaServer started successfully ======");
        }
        catch (ParaFlowException pe) {
            pe.handle();
        }
    }

    private void initMetadata()
    {
        // TODO init meta data
//        //find whether table exit
//        String allTableSql =
//                "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;";
//        ResultList resAllTable = dbConnection.sqlQuery(allTableSql, 1);
//        //result
//        ArrayList<String> result = new ArrayList<>();
//        int size = resAllTable.size();
//        for (int i = 0; i < size; i++) {
//            result.add(resAllTable.get(i).get(0));
//        }
//        //no table then create all tables
//        MetaProto.StatusType statusType;
//        if (!(result.contains("blockindex")) && !(result.contains("colmodel"))
//                && !(result.contains("dbmodel"))
//                && !(result.contains("dbparammodel")) && !(result.contains("fiberfuncmodel"))
//                && !(result.contains("storageformatmodel")) && !(result.contains("tblmodel"))
//                && !(result.contains("tblparammodel")) && !(result.contains("tblprivmodel"))
//                && !(result.contains("usermodel")) && !(result.contains("vermodel"))) {
//            //VerModel
//            String createVerModelSql = SQL.createVerModelSql;
//            int resCreateVerModel = dbConnection.sqlUpdate(createVerModelSql);
//            //UserModel
//            String createUserModelSql = SQL.createUserModelSql;
//            int resCreateUserModel = dbConnection.sqlUpdate(createUserModelSql);
//            //DbModel
//            String createDbModelSql = SQL.createDbModelSql;
//            int resCreateDbModel = dbConnection.sqlUpdate(createDbModelSql);
//            //TblModel
//            String createTblModelSql = SQL.createTblModelSql;
//            int resCreateTblModel = dbConnection.sqlUpdate(createTblModelSql);
//            //ColModel
//            String createColModelSql = SQL.createColModelSql;
//            int resCreateColModel = dbConnection.sqlUpdate(createColModelSql);
//            //DbParamModel
//            String createDbParamModelSql = SQL.createDbParamModelSql;
//            int resCreateDbParamModel = dbConnection.sqlUpdate(createDbParamModelSql);
//            //TblParamModel
//            String createTblParamModelSql = SQL.createTblParamModelSql;
//            int resCreateTblParamModel = dbConnection.sqlUpdate(createTblParamModelSql);
//            //TblPrivModel
//            String createTblPrivModelSql = SQL.createTblPrivModelSql;
//            int resCreateTblPrivModel = dbConnection.sqlUpdate(createTblPrivModelSql);
//            //StorageFormatModel
//            String createStorageFormatModelSql = SQL.createStorageFormatModelSql;
//            int resCreateStorageFormatModel = dbConnection.sqlUpdate(createStorageFormatModelSql);
//            //FiberFuncModel
//            String createFiberFuncModelSql = SQL.createFiberFuncModelSql;
//            int resCreateFiberFuncModel = dbConnection.sqlUpdate(createFiberFuncModelSql);
//            //BlockIndex
//            String createBlockIndexSql = SQL.createBlockIndexSql;
//            int resCreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
//            if (resCreateVerModel == 0 && resCreateDbModel == 0 && resCreateDbParamModel == 0
//                    && resCreateTblModel == 0 && resCreateTblParamModel == 0
//                    && resCreateTblPrivModel == 0 && resCreateStorageFormatModel == 0
//                    && resCreateColModel == 0 && resCreateFiberFuncModel == 0
//                    && resCreateBlockIndex == 0 && resCreateUserModel == 0) {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
//                System.out.println("Meta table create status is : " + statusType.getStatus());
//            }
//            else {
//                statusType = MetaProto.StatusType.newBuilder().setStatus(
//                        MetaProto.StatusType.State.META_TABLE_CREATE_FAIL).build();
//                System.err.println(statusType.getClass().getName() + ": " + statusType.getStatus());
//                System.exit(0);
//            }
//        }
//        else if (result.contains("blockindex")
//                && result.contains("colmodel") && result.contains("dbmodel")
//                && result.contains("dbparammodel") && result.contains("fiberfuncmodel")
//                && result.contains("storageformatmodel") && result.contains("tblmodel")
//                && result.contains("tblparammodel") && result.contains("tblprivmodel")
//                && result.contains("usermodel") && result.contains("vermodel")) {
//            statusType = MetaProto.StatusType.newBuilder().setStatus(
//                    MetaProto.StatusType.State.META_TABLE_ALREADY_EXISTS).build();
//            System.out.println("Meta table status is : " + statusType.getStatus());
//        }
//        else {
//            statusType = MetaProto.StatusType.newBuilder().setStatus(
//                    MetaProto.StatusType.State.META_TABLE_BROKEN).build();
//            System.out.println("Meta table create status is : " + statusType.getStatus());
//        }
    }

    /**
     * Stop meta server and stop all sub-modules
     * 1. Stop rpcServer
     * 2. Stop db connection
     * */
    private void stop()
    {
        if (server != null) {
            logger.info("****** MetaServer shutting down...");
            System.out.println("****** MetaServer shutting down...");
            server.shutdown();
            ConnectionPool.INSTANCE().close();
            logger.info("****** MetaServer has shut down");
            System.out.println("****** MetaServer shutting down");
        }
    }

    /**
     * Await for interrupted signal to stop server
     * */
    private void blockUntilTermination()
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
        MetaServer server = new MetaServer(args[0]);
        server.start();
    }
}
