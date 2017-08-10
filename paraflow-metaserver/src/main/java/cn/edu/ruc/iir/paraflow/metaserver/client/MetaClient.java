package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaGrpc;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaClient
{
    private static final Logger logger = Logger.getLogger(MetaClient.class.getName());

    private final ManagedChannel channel;
    private final MetaGrpc.MetaBlockingStub metaBlockingStub;

    public MetaClient(String host, int port)
    {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    private MetaClient(ManagedChannel channel)
    {
        this.channel = channel;
        this.metaBlockingStub = MetaGrpc.newBlockingStub(channel);
        logger.info("***Client started.");
    }

    public void shutdown(int pollSecs) throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(pollSecs, TimeUnit.SECONDS);
    }

    public MetaProto.StringListType listDatabases()
    {
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listDatabases(none);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Database list : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType listTables()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listTables(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Table list : " + stringList);
        return stringList;
    }

    public MetaProto.DbParam getDatabase()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.DbParam database;
        try {
            database = metaBlockingStub.getDatabase(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            database = MetaProto.DbParam.newBuilder().build();
            return database;
        }
        logger.info("Database is : " + database);
        return  database;
    }

    public MetaProto.TblParam getTable()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.DbTblParam databaseTable = MetaProto.DbTblParam.newBuilder().setDatabase(databaseName).setTable(tableName).build();
        MetaProto.TblParam table;
        try {
            table = metaBlockingStub.getTable(databaseTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            table = MetaProto.TblParam.newBuilder().build();
            return table;
        }
        logger.info("Table is : " + table);
        return table;
    }

    public MetaProto.ColParam getColumn()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("defult").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.ColNameParam columnName = MetaProto.ColNameParam.newBuilder().setColumn("name").build();
        MetaProto.DbTblColParam databaseTableColumn = MetaProto.DbTblColParam.newBuilder().setDatabase(databaseName).setTable(tableName).setColumn(columnName).build();
        MetaProto.ColParam column;
        try {
            column = metaBlockingStub.getColumn(databaseTableColumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            column = MetaProto.ColParam.newBuilder().build();
            return column;
        }
        logger.info("Column is : " + column);
        return column;
    }

    public MetaProto.StatusType createDatabase()
    {
        //MetaProto.UserParam user = MetaProto.UserParam.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbParam database = MetaProto.DbParam.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserName("Alice").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createDatabase(database);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create database status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTable()
    {
//        MetaProto.UserParam user = MetaProto.UserParam.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
//        MetaProto.DbParam database = MetaProto.DbParam.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
//        MetaProto.ColParam column0 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
//        MetaProto.ColParam column1 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
//        MetaProto.ColParam column2 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
//        MetaProto.ColParam column3 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
//        MetaProto.ColParam column4 = MetaProto.ColParam.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(0).build();
//        MetaProto.ColumnListType columns = MetaProto.ColumnListType.newBuilder().addColumn(0, column0).addColumn(1, column1).addColumn(2, column2).addColumn(3, column3).addColumn(4, column4).build();
        MetaProto.TblParam table = MetaProto.TblParam.newBuilder().setDbName("default").setCreateTime(20170807).setLastAccessTime(20170807).setUserName("Alice").setTblName("employee").setTblType(0).setFiberColId(-1).setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default/employee").setStorageFormatId(1).setFiberFuncId(1).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTable(table);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Create table status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType deleteDatabase()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.deleteDatabase(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Delete database status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType deleteTable()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.DbTblParam databaseTable = MetaProto.DbTblParam.newBuilder().setDatabase(databaseName).setTable(tableName).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.deleteTable(databaseTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Delete table status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType renameDatabase()
    {
        MetaProto.RenameDbParam renameDatabase = MetaProto.RenameDbParam.newBuilder().setOldName("default").setNewName("defaultnew").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameDatabase(renameDatabase);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename database status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType renameTable()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.RenameTblParam renameTable = MetaProto.RenameTblParam.newBuilder().setDatabase(databaseName).setOldName("employee").setNewName("employeenew").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameTable(renameTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename table status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType renameColumn()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.RenameColParam renameColumn = MetaProto.RenameColParam.newBuilder().setDatabase(databaseName).setTable(tableName).setOldName("name").setNewName("firstname").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameColumn(renameColumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createDbParam()
    {
        MetaProto.CreateDbParamParam createDbParam = MetaProto.CreateDbParamParam.newBuilder().setDbName("default").setParamKey("He").setParamValue("is a student").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createDbParam(createDbParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTblParam()
    {
        MetaProto.CreateTblParamParam createTblParam = MetaProto.CreateTblParamParam.newBuilder().setTblName("employee").setParamKey("She").setParamValue("is smart and beautiful").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTblParam(createTblParam);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createTblPriv()
    {
        MetaProto.CreateTblPrivParam createTblPriv = MetaProto.CreateTblPrivParam.newBuilder().setTblName("employee").setUserName("Alice").setPrivType(1).setGrantTime(20170810).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTblPriv(createTblPriv);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createStorageFormat()
    {
        MetaProto.CreateStorageFormatParam createStorageFormat = MetaProto.CreateStorageFormatParam.newBuilder().setStorageFormatName("StorageFormat").setCompression("Compression").setSerialFormat("SerialFormat").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createStorageFormat(createStorageFormat);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createFiberFunc()
    {
        MetaProto.CreateFiberFuncParam createFiberFunc = MetaProto.CreateFiberFuncParam.newBuilder().setFiberFuncName("FiberFuncName").setFiberFuncContent("FiberFuncContent").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createFiberFunc(createFiberFunc);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createBlockIndex()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder().setValue(1234567890).build();
        MetaProto.CreateBlockIndexParam addBlockIndex = MetaProto.CreateBlockIndexParam.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setBeginTime("20170807 13:50:00").setEndTime("20170807 13:55:00").setPath("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createBlockIndex(addBlockIndex);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Add block index status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StatusType createUser()
    {
        MetaProto.CreateUserParam createUser = MetaProto.CreateUserParam.newBuilder().setUserName("Alice").setCreateTime(20180810).setLastVisitTime(20170810).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createUser(createUser);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            status = MetaProto.StatusType.newBuilder().build();
            return status;
        }
        logger.info("Rename column status is : " + status.getStatus());
        return status;
    }

    public MetaProto.StringListType filterBlockIndex()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FilterBlockIndexParam filterBlockIndex = MetaProto.FilterBlockIndexParam.newBuilder().setDatabase(databaseName).setTable(tableName).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.filterBlockIndex(filterBlockIndex);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Filter block paths by time is : " + stringList);
        return stringList;
    }

    public MetaProto.StringListType filterBlockIndexByFiber()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder().setValue(1234567890).build();
        MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiber = MetaProto.FilterBlockIndexByFiberParam.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.filterBlockIndexByFiber(filterBlockIndexByFiber);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            stringList = MetaProto.StringListType.newBuilder().build();
            return stringList;
        }
        logger.info("Filter block paths is : " + stringList);
        return stringList;
    }
}
