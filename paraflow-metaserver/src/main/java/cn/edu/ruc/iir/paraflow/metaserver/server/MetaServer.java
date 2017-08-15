package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.service.MetaService;
import cn.edu.ruc.iir.paraflow.metaserver.connection.DBConnection;
import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Optional;

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
        MetaConfig metaConfig = null;
        try {
            metaConfig = new MetaConfig(metaConfigPath);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }

        // connect database
        DBConnection dbConnection = DBConnection.getConnectionInstance();
        dbConnection.connect(
                metaConfig.getDBDriver(),
                metaConfig.getDBHost(),
                metaConfig.getDBUser(),
                metaConfig.getDBPassword());

        // TODO rewrite this code block into a new function
        //find whether table exit
        try {
            String allTableSql = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;";
            Optional optAllTable = dbConnection.sqlQuery(allTableSql);
            ResultSet resAllTable = (ResultSet) optAllTable.get();
            //result
            ArrayList<String> result = new ArrayList<>();
            while (resAllTable.next()) {
                result.add(resAllTable.getString(1));
            }
            //no table then create all tables
            MetaProto.StatusType statusType;
            if (!(result.contains("blockindex")) && !(result.contains("colmodel")) && !(result.contains("dbmodel"))
                    && !(result.contains("dbparammodel")) && !(result.contains("fiberfuncmodel"))
                    && !(result.contains("storageformatmodel")) && !(result.contains("tblmodel"))
                    && !(result.contains("tblparammodel")) && !(result.contains("tblprivmodel"))
                    && !(result.contains("usermodel")) && !(result.contains("vermodel"))) {
                //VerModel
                String createVerModelSql = "CREATE TABLE vermodel (verid int);";
                Optional<Integer> optCreateVerModel = dbConnection.sqlUpdate(createVerModelSql);
                int resCreateVerModel = (int) optCreateVerModel.get();
                //UserModel
                String createUserModelSql = "CREATE TABLE usermodel (userid SERIAL primary key,username varchar(50),createtime int,lastvisittime int);";
                Optional<Integer> optCreateUserModel = dbConnection.sqlUpdate(createUserModelSql);
                int resCreateUserModel = (int) optCreateUserModel.get();
                //DbModel
                String createDbModelSql = "CREATE TABLE dbmodel (dbid SERIAL primary key,dbname varchar(20),userid int REFERENCES usermodel(userid),locationurl varchar(200));";
                Optional<Integer> optCreateDbModel = dbConnection.sqlUpdate(createDbModelSql);
                int resCreateDbModel = (int) optCreateDbModel.get();
                //TblModel
                String createTblModelSql = "CREATE TABLE tblmodel (tblid SERIAL primary key,dbid int REFERENCES dbmodel(dbid),tblname varchar(50),tbltype int,userid int REFERENCES usermodel(userid),createtime int,lastaccesstime int,locationUrl varchar(100),storageformatid int,fiberColId int,fiberfuncid int);";
                Optional<Integer> optCreateTblModel = dbConnection.sqlUpdate(createTblModelSql);
                int resCreateTblModel = (int) optCreateTblModel.get();
                //ColModel
                String createColModelSql = "CREATE TABLE colmodel (colid SERIAL primary key,colIndex int,dbid int REFERENCES dbmodel(dbid),tblid int REFERENCES tblmodel(tblid),colName varchar(50),colType varchar(50),dataType varchar(50));";
                Optional<Integer> optCreateColModel = dbConnection.sqlUpdate(createColModelSql);
                int resCreateColModel = (int) optCreateColModel.get();
                //DbParamModel
                String createDbParamModelSql = "CREATE TABLE dbparammodel (dbid int REFERENCES dbmodel(dbid),paramkey varchar(100),paramvalue varchar(200));";
                Optional<Integer> optCreateDbParamModel = dbConnection.sqlUpdate(createDbParamModelSql);
                int resCreateDbParamModel = (int) optCreateDbParamModel.get();
                //TblParamModel
                String createTblParamModelSql = "CREATE TABLE tblparammodel (tblid int REFERENCES tblmodel(tblid),paramkey varchar(100),paramvalue varchar(200));";
                Optional<Integer> optCreateTblParamModel = dbConnection.sqlUpdate(createTblParamModelSql);
                int resCreateTblParamModel = (int) optCreateTblParamModel.get();
                //TblPrivModel
                String createTblPrivModelSql = "CREATE TABLE tblprivmodel (tblprivid SERIAL primary key,tblid int REFERENCES tblmodel(tblid),userid int REFERENCES usermodel(userid),privtype int,granttime int);";
                Optional<Integer> optCreateTblPrivModel = dbConnection.sqlUpdate(createTblPrivModelSql);
                int resCreateTblPrivModel = (int) optCreateTblPrivModel.get();
                //StorageFormatModel
                String createStorageFormatModelSql = "CREATE TABLE storageformatmodel (storageformatid SERIAL primary key,storageformatname varchar(50),compression varchar(50),serialformat varchar(50));";
                Optional<Integer> optCreateStorageFormatModel = dbConnection.sqlUpdate(createStorageFormatModelSql);
                int resCreateStorageFormatModel = (int) optCreateStorageFormatModel.get();
                //FiberFuncModel
                String createFiberFuncModelSql = "CREATE TABLE fiberfuncmodel (fiberfuncid SERIAL primary key,fiberfuncname varchar(50),fiberfunccontent bytea);";
                Optional<Integer> optCreateFiberFuncModel = dbConnection.sqlUpdate(createFiberFuncModelSql);
                int resCreateFiberFuncModel = (int) optCreateFiberFuncModel.get();
                //BlockIndex
                String createBlockIndexSql = "CREATE TABLE blockindex (blockindexid SERIAL primary key,tblid int REFERENCES tblmodel(tblid),fibervalue int,timebegin int,timeend int,timezone varchar(50),blockpath varchar(100));";
                Optional<Integer> optCreateBlockIndex = dbConnection.sqlUpdate(createBlockIndexSql);
                int resCreateBlockIndex = (int) optCreateBlockIndex.get();
                if (resCreateVerModel == 0 && resCreateDbModel == 0 && resCreateDbParamModel == 0
                        && resCreateTblModel == 0 && resCreateTblParamModel == 0
                        && resCreateTblPrivModel == 0 && resCreateStorageFormatModel == 0
                        && resCreateColModel == 0 && resCreateFiberFuncModel == 0
                        && resCreateBlockIndex == 0 && resCreateUserModel == 0) {
                    statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
                    System.out.println("Meta table create status is : " + statusType.getStatus());
                }
                else {
                    statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.META_TABLE_CREATE_FAIL).build();
                    System.err.println(statusType.getClass().getName() + ": " + statusType.getStatus());
                    System.exit(0);
                }
            }
            else if (result.contains("blockindex") && result.contains("colmodel") && result.contains("dbmodel")
                    && result.contains("dbparammodel") && result.contains("fiberfuncmodel")
                    && result.contains("storageformatmodel") && result.contains("tblmodel")
                    && result.contains("tblparammodel") && result.contains("tblprivmodel")
                    && result.contains("usermodel") && result.contains("vermodel")) {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.META_TABLE_ALREADY_EXISTS).build();
                System.out.println("Meta table status is : " + statusType.getStatus());
            }
            else {
                statusType = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.META_TABLE_BROKEN).build();
                System.out.println("Meta table create status is : " + statusType.getStatus());
            }
        }
        catch (java.sql.SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
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
