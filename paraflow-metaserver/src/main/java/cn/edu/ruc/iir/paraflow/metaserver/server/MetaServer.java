package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;

import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;

import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;

import cn.edu.ruc.iir.paraflow.metaserver.utils.CreateSQL;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaServer
{
    private static final Logger logger = LogManager.getLogger(MetaServer.class);

    private Server server;
    private String metaConfigPath = "";
    private static MetaServer serverInstance = null;
    DBConnection dbConnection = DBConnection.getConnectionInstance();

    public MetaServer(String metaConfigPath)
    {
        this.metaConfigPath = metaConfigPath;
    }

    public MetaServer()
    {
    }

    public static synchronized MetaServer getServerInstance(String metaConfigPath)
    {
        if (serverInstance == null) {
            serverInstance = new MetaServer(metaConfigPath);
        }
        return serverInstance;
    }

    public static synchronized MetaServer getServerInstance()
    {
        if (serverInstance == null) {
            serverInstance = new MetaServer();
        }
        return serverInstance;
    }

    /**
     * Start meta server.
     * 1. Start rpc server;
     * 2. Connect to database.
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
        catch (java.io.IOException e) {
            throw new RPCServerIOException(port);
        }
        Runtime.getRuntime().addShutdownHook(
                new Thread(MetaServer.this::stop)
        );
        logger.info("****** RPC server started at port " + port);
        System.out.println("****** RPC server started at port " + port);

        // get config instance
        MetaConfig metaConfig = null;
        try {
            metaConfig = new MetaConfig(metaConfigPath);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }

        // connect database
        dbConnection.connect(
                metaConfig.getDBDriver(),
                metaConfig.getDBHost(),
                metaConfig.getDBUser(),
                metaConfig.getDBPassword());
        //init meta table
        metaTableInit();
        logger.info("****** Database connected successfully");
        System.out.println("****** Database connected successfully");

        logger.info("====== MetaServer started successfully ======");
        System.out.println("====== MetaServer started successfully ======");
    }

    public void metaTableInit()
    {
        //find whether table exit
        String allTableSql =
                "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;";
        ResultList resAllTable = dbConnection.sqlQuery(allTableSql, 1);
        //result
        ArrayList<String> result = new ArrayList<>();
        int size = resAllTable.size();
        for (int i = 0; i < size; i++) {
            result.add(resAllTable.get(i).get(0));
        }
        //no table then create all tables
        MetaProto.StatusType statusType;
        if (!(result.contains("blockindex")) && !(result.contains("colmodel"))
                && !(result.contains("dbmodel"))
                && !(result.contains("dbparammodel")) && !(result.contains("fiberfuncmodel"))
                && !(result.contains("storageformatmodel")) && !(result.contains("tblmodel"))
                && !(result.contains("tblparammodel")) && !(result.contains("tblprivmodel"))
                && !(result.contains("usermodel")) && !(result.contains("vermodel"))) {
            //VerModel
            String createVerModelSql = CreateSQL.createVerModelSql;
            int resCreateVerModel = dbConnection.sqlUpdate(createVerModelSql);
            //UserModel
            String createUserModelSql = CreateSQL.createUserModelSql;
            int resCreateUserModel = dbConnection.sqlUpdate(createUserModelSql);
            //DbModel
            String createDbModelSql = CreateSQL.createDbModelSql;
            int resCreateDbModel = dbConnection.sqlUpdate(createDbModelSql);
            //StorageFormatModel
            String createStorageFormatModelSql = CreateSQL.createStorageFormatModelSql;
            int resCreateStorageFormatModel = dbConnection.sqlUpdate(createStorageFormatModelSql);
            //FiberFuncModel
            String createFiberFuncModelSql = CreateSQL.createFiberFuncModelSql;
            int resCreateFiberFuncModel = dbConnection.sqlUpdate(createFiberFuncModelSql);
            //TblModel
            String createTblModelSql = CreateSQL.createTblModelSql;
            int resCreateTblModel = dbConnection.sqlUpdate(createTblModelSql);
            //ColModel
            String createColModelSql = CreateSQL.createColModelSql;
            int resCreateColModel = dbConnection.sqlUpdate(createColModelSql);
            //DbParamModel
            String createDbParamModelSql = CreateSQL.createDbParamModelSql;
            int resCreateDbParamModel = dbConnection.sqlUpdate(createDbParamModelSql);
            //TblParamModel
            String createTblParamModelSql = CreateSQL.createTblParamModelSql;
            int resCreateTblParamModel = dbConnection.sqlUpdate(createTblParamModelSql);
            //TblPrivModel
            String createTblPrivModelSql = CreateSQL.createTblPrivModelSql;
            int resCreateTblPrivModel = dbConnection.sqlUpdate(createTblPrivModelSql);
            //BlockIndex
            String createBlockIndexSql = CreateSQL.createBlockIndexSql;
            int resCreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
            //init vermodel
            String insertVerSql = "INSERT INTO vermodel (vername) VALUES('1.0-alpha1');";
            int resInsertVer = dbConnection.sqlUpdate(insertVerSql);
            //init fiberfuncmodel
            String insertFiberFuncSql = "INSERT INTO fiberfuncmodel (fiberfuncname,fiberfunccontent) VALUES('none','none');";
            int resInsertFiberFunc = dbConnection.sqlUpdate(insertFiberFuncSql);
            if (resCreateVerModel == 0 && resCreateDbModel == 0 && resCreateDbParamModel == 0
                    && resCreateTblModel == 0 && resCreateTblParamModel == 0
                    && resCreateTblPrivModel == 0 && resCreateStorageFormatModel == 0
                    && resCreateColModel == 0 && resCreateFiberFuncModel == 0
                    && resCreateBlockIndex == 0 && resCreateUserModel == 0
                    && resInsertVer == 1 && resInsertFiberFunc == 1) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                System.out.println("Meta table create status is : " + statusType.getStatus());
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(
                        MetaProto.StatusType.State.META_TABLE_CREATE_FAIL).build();
                System.err.println(statusType.getClass().getName() + ": " + statusType.getStatus());
                System.exit(0);
            }
        }
        else if (result.contains("blockindex")
                && result.contains("colmodel") && result.contains("dbmodel")
                && result.contains("dbparammodel") && result.contains("fiberfuncmodel")
                && result.contains("storageformatmodel") && result.contains("tblmodel")
                && result.contains("tblparammodel") && result.contains("tblprivmodel")
                && result.contains("usermodel") && result.contains("vermodel")) {
            statusType = MetaProto.StatusType.newBuilder().setStatus(
                    MetaProto.StatusType.State.META_TABLE_ALREADY_EXISTS).build();
            System.out.println("Meta table status is : " + statusType.getStatus());
        }
        else {
            statusType = MetaProto.StatusType.newBuilder().setStatus(
                    MetaProto.StatusType.State.META_TABLE_BROKEN).build();
            System.out.println("Meta table create status is : " + statusType.getStatus());
        }
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
        MetaServer server = new MetaServer(args[0]);
        try {
            server.start(10012);
            server.blockUntilTermination();
        }
        catch (RPCServerIOException e) {
            server.stop();
            e.printStackTrace();
        }
    }
}
