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

    public void listDatabases()
    {
        MetaProto.NoneType none = MetaProto.NoneType.newBuilder().build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listDatabases(none);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Database list : " + stringList);
    }

    public void listTables()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.listTables(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Table list : " + stringList);
    }

    public void getDatabase()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.DbModel database;
        try {
            database = metaBlockingStub.getDatabase(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Database is : " + database);
    }

    public void getTable()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.DbTblParam databaseTable = MetaProto.DbTblParam.newBuilder().setDatabase(databaseName).setTable(tableName).build();
        MetaProto.TblModel table;
        try {
            table = metaBlockingStub.getTable(databaseTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Table is : " + table);
    }

    public void getColumn()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("defult").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.ColNameParam columnName = MetaProto.ColNameParam.newBuilder().setColumn("name").build();
        MetaProto.DbTblColParam databaseTableColumn = MetaProto.DbTblColParam.newBuilder().setDatabase(databaseName).setTable(tableName).setColumn(columnName).build();
        MetaProto.ColModel column;
        try {
            column = metaBlockingStub.getColumn(databaseTableColumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Column is : " + column);
    }

    public void createDatabase()
    {
        MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbModel database = MetaProto.DbModel.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createDatabase(database);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create database status is : " + status.getStatus());
    }

    public void createTable()
    {
        MetaProto.UserModel user = MetaProto.UserModel.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.DbModel database = MetaProto.DbModel.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.ColModel column0 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        MetaProto.ColModel column1 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        MetaProto.ColModel column2 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        MetaProto.ColModel column3 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        MetaProto.ColModel column4 = MetaProto.ColModel.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(0).build();
        MetaProto.ColumnListType columns = MetaProto.ColumnListType.newBuilder().addColumn(0, column0).addColumn(1, column1).addColumn(2, column2).addColumn(3, column3).addColumn(4, column4).build();
        MetaProto.TblModel table = MetaProto.TblModel.newBuilder().setDatabase(database).setCreationTime(20170807).setLastAccessTime(20170807).setOwner(user).setTableName("employee").setTableLocationUri("hdfs:/127.0.0.1:9000/warehouse/default/employee").setColumns(columns).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createTable(table);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create table status is : " + status.getStatus());
    }

    public void deleteDatabase()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.deleteDatabase(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Delete database status is : " + status.getStatus());
    }

    public void deleteTable()
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
            return;
        }
        logger.info("Delete table status is : " + status.getStatus());
    }

    public void renameDatabase()
    {
        MetaProto.RenameDbParam renameDatabase = MetaProto.RenameDbParam.newBuilder().setOldName("default").setNewName("defaultnew").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameDatabase(renameDatabase);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Rename database status is : " + status.getStatus());
    }

    public void renameTable()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.RenameTblParam renameTable = MetaProto.RenameTblParam.newBuilder().setDatabase(databaseName).setOldName("employee").setNewName("employeenew").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.renameTable(renameTable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Rename table status is : " + status.getStatus());
    }

    public void renameColumn()
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
            return;
        }
        logger.info("Rename column status is : " + status.getStatus());
    }

    public void createFiber()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberModel fiber = MetaProto.FiberModel.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(1234567890).build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.createFiber(fiber);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create fiber status is : " + status.getStatus());
    }

    public void listFiberValues()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberModel fiber = MetaProto.FiberModel.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(1234567890).build();
        MetaProto.LongListType longList;
        try {
            longList = metaBlockingStub.listFiberValues(fiber);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Fiber values list is : " + longList);
    }

    public void addBlockIndex()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder().setValue(1234567890).build();
        MetaProto.AddBlockIndexParam addBlockIndex = MetaProto.AddBlockIndexParam.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setBeginTime("20170807 13:50:00").setEndTime("20170807 13:55:00").setPath("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").build();
        MetaProto.StatusType status;
        try {
            status = metaBlockingStub.addBlockIndex(addBlockIndex);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Add block index status is : " + status.getStatus());
    }

    public void filterBlockPathsByTime()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FilterBlockPathsByTimeParam filterBlockPathsByTime = MetaProto.FilterBlockPathsByTimeParam.newBuilder().setDatabase(databaseName).setTable(tableName).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringListType stringList;
        try {
            stringList = metaBlockingStub.filterBlockPathsByTime(filterBlockPathsByTime);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths by time is : " + stringList);
    }

    public void filterBlockPaths()
    {
        MetaProto.DbNameParam databaseName = MetaProto.DbNameParam.newBuilder().setDatabase("default").build();
        MetaProto.TblNameParam tableName = MetaProto.TblNameParam.newBuilder().setTable("employee").build();
        MetaProto.FiberValueType fiberValue = MetaProto.FiberValueType.newBuilder().setValue(1234567890).build();
        MetaProto.FilterBlockPathsParam filterBlockPaths = MetaProto.FilterBlockPathsParam.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringListType stringlist;
        try {
            stringlist = metaBlockingStub.filterBlockPaths(filterBlockPaths);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths is : " + stringlist);
    }
}
