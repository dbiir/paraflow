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

//    public void sayHi(String name)
//    {
//        logger.info("Say hi to server: " + name);
//        MetaProto.Request request = MetaProto.Request.newBuilder().setName(name).build();
//        MetaProto.Response response;
//        try {
//            response = metaBlockingStub.sayHi(request);
//        }
//        catch (StatusRuntimeException e) {
//            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
//            return;
//        }
//        logger.info("Greeting: " + response.getStatus());
//    }
       public void listDatabases()
     {
        MetaProto.None none = MetaProto.None.newBuilder().build();
        MetaProto.StringList string_list;
        try {
            string_list = metaBlockingStub.listDatabases(none);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Database list : " + string_list);
    }
    public void listTables()
    {
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.StringList string_list;
        try {
            string_list = metaBlockingStub.listTables(databaseName);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Table list : " + string_list);
    }
    public void getDatabase()
    {
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.Database database;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.DatabaseTable databaseTable = MetaProto.DatabaseTable.newBuilder().setDatabase(databaseName).setTable(tableName).build();
        MetaProto.Table table;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("defult").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.ColumnName columnName = MetaProto.ColumnName.newBuilder().setColumn("name").build();
        MetaProto.DatabaseTableColumn databaseTableColumn = MetaProto.DatabaseTableColumn.newBuilder().setDatabase(databaseName).setTable(tableName).setColumn(columnName).build();
        MetaProto.Column column;
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
        MetaProto.User user = MetaProto.User.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.Database database = MetaProto.Database.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.Status status;
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
        MetaProto.User user = MetaProto.User.newBuilder().setUserName("Alice").setUserPass("123456").setRoleName("admin").setCreationTime(20170807).setLastVisitTime(20170807).build();
        MetaProto.Database database = MetaProto.Database.newBuilder().setName("default").setLocationUri("hdfs:/127.0.0.1:9000/warehouse/default").setUser(user).build();
        MetaProto.Column column0 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("name").setDataType("varchar(20)").setColIndex(0).build();
        MetaProto.Column column1 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("age").setDataType("integer").setColIndex(1).build();
        MetaProto.Column column2 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("salary").setDataType("double").setColIndex(2).build();
        MetaProto.Column column3 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("check-in").setDataType("timestamp").setColIndex(3).build();
        MetaProto.Column column4 = MetaProto.Column.newBuilder().setDatabasename("default").setTablename("employee").setColName("comment").setDataType("char(10)").setColIndex(0).build();
        MetaProto.Columns columns = MetaProto.Columns.newBuilder().addColumn(0,column0).addColumn(1,column1).addColumn(2,column2).addColumn(3,column3).addColumn(4,column4).build();
        MetaProto.Table table = MetaProto.Table.newBuilder().setDatabase(database).setCreationTime(20170807).setLastAccessTime(20170807).setOwner(user).setTableName("employee").setTableLocationUri("hdfs:/127.0.0.1:9000/warehouse/default/employee").setColumns(columns).build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.DatabaseTable databaseTable = MetaProto.DatabaseTable.newBuilder().setDatabase(databaseName).setTable(tableName).build();
        MetaProto.Status status;
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
        MetaProto.RenameDatabase renameDatabase = MetaProto.RenameDatabase.newBuilder().setOldName("default").setNewName("defaultnew").build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.RenameTable renameTable = MetaProto.RenameTable.newBuilder().setDatabase(databaseName).setOldName("employee").setNewName("employeenew").build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.RenameColumn renameColumn = MetaProto.RenameColumn.newBuilder().setDatabase(databaseName).setTable(tableName).setOldName("name").setNewName("firstname").build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.Fiber fiber = MetaProto.Fiber.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(1234567890).build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.Fiber fiber = MetaProto.Fiber.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(1234567890).build();
        MetaProto.LongList long_list;
        try {
            long_list = metaBlockingStub.listFiberValues(fiber);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Fiber values list is : " + long_list);
    }
    public void addBlockIndex()
    {
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.FiberValue fiberValue = MetaProto.FiberValue.newBuilder().setValue(1234567890).build();
        MetaProto.AddBlockIndex addBlockIndex = MetaProto.AddBlockIndex.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setBeginTime("20170807 13:50:00").setEndTime("20170807 13:55:00").setPath("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").build();
        MetaProto.Status status;
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
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.FilterBlockPathsByTime filterBlockPathsByTime = MetaProto.FilterBlockPathsByTime.newBuilder().setDatabase(databaseName).setTable(tableName).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringList stringList;
        try {
            stringList = metaBlockingStub.filterBlockPathsByTime(filterBlockPathsByTime);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths by time is : " + stringList);
    }
    public void filterBlockPaths() {
        MetaProto.DatabaseName databaseName = MetaProto.DatabaseName.newBuilder().setDatabase("default").build();
        MetaProto.TableName tableName = MetaProto.TableName.newBuilder().setTable("employee").build();
        MetaProto.FiberValue fiberValue = MetaProto.FiberValue.newBuilder().setValue(1234567890).build();
        MetaProto.FilterBlockPaths filterBlockPaths = MetaProto.FilterBlockPaths.newBuilder().setDatabase(databaseName).setTable(tableName).setValue(fiberValue).setTimelow("20170807 13:50:00").setTimehigh("20170807 13:55:00").build();
        MetaProto.StringList stringlist;
        try {
            stringlist = metaBlockingStub.filterBlockPaths(filterBlockPaths);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths is : " + stringlist);
    }
}
