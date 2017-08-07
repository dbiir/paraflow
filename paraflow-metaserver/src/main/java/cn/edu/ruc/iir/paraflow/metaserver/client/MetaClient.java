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
        MetaProto.DatabaseName databasename = MetaProto.DatabaseName.newBuilder().build();
        MetaProto.StringList string_list;
        try {
            string_list = metaBlockingStub.listTables(databasename);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Table list : " + string_list);
    }
    public void getDatabase()
    {
        MetaProto.DatabaseName databasename = MetaProto.DatabaseName.newBuilder().build();
        MetaProto.Database database;
        try {
            database = metaBlockingStub.getDatabase(databasename);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Database is : " + database);
    }
    public void getTable()
    {
        MetaProto.DatabaseTable databasetable = MetaProto.DatabaseTable.newBuilder().build();
        MetaProto.Table table;
        try {
            table = metaBlockingStub.getTable(databasetable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Table is : " + table);
    }
    public void getColumn()
    {
        MetaProto.DatabaseTableColumn databasetablecolumn = MetaProto.DatabaseTableColumn.newBuilder().build();
        MetaProto.Column column;
        try {
            column = metaBlockingStub.getColumn(databasetablecolumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Column is : " + column);
    }
    public void createDatabase()
    {
        MetaProto.Database database = MetaProto.Database.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.createDatabase(database);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create database status is : " + status);
    }
    public void createTable()
    {
        MetaProto.Table table = MetaProto.Table.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.createTable(table);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create table status is : " + status);
    }
    public void deleteDatabase()
    {
        MetaProto.DatabaseName databasename = MetaProto.DatabaseName.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.deleteDatabase(databasename);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Delete database status is : " + status);
    }
    public void deleteTable()
    {
        MetaProto.DatabaseTable databasetable = MetaProto.DatabaseTable.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.deleteTable(databasetable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Delete table status is : " + status);
    }
    public void renameDatabase()
    {
        MetaProto.RenameDatabase renamedatabase = MetaProto.RenameDatabase.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.renameDatabase(renamedatabase);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Rename database status is : " + status);
    }
    public void renameTable()
    {
        MetaProto.RenameTable renametable = MetaProto.RenameTable.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.renameTable(renametable);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Rename table status is : " + status);
    }
    public void renameColumn()
    {
        MetaProto.RenameColumn renamecolumn = MetaProto.RenameColumn.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.renameColumn(renamecolumn);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Rename column status is : " + status);
    }
    public void createFiber()
    {
        MetaProto.Fiber fiber = MetaProto.Fiber.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.createFiber(fiber);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Create fiber status is : " + status);
    }
    public void listFiberValues()
    {
        MetaProto.Fiber fiber = MetaProto.Fiber.newBuilder().build();
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
        MetaProto.AddBlockIndex addblockindex = MetaProto.AddBlockIndex.newBuilder().build();
        MetaProto.Status status;
        try {
            status = metaBlockingStub.addBlockIndex(addblockindex);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Add block index status is : " + status);
    }
    public void filterBlockPathsByTime()
    {
        MetaProto.FilterBlockPathsByTime filterblockpathsbytime = MetaProto.FilterBlockPathsByTime.newBuilder().build();
        MetaProto.StringList stringlist;
        try {
            stringlist = metaBlockingStub.filterBlockPathsByTime(filterblockpathsbytime);
        }
        catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths by time is : " + stringlist);
    }
    public void filterBlockPaths() {
        MetaProto.FilterBlockPaths filterblockpaths = MetaProto.FilterBlockPaths.newBuilder().build();
        MetaProto.StringList stringlist;
        try {
            stringlist = metaBlockingStub.filterBlockPaths(filterblockpaths);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Filter block paths is : " + stringlist);
    }
//    public static void main(String[] args) throws Exception {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        try {
//      /* Access a service running on the local machine on port 10012 */
//            client.listDatabases();
////            client.listDatabases();
////            client.listTables();
////            client.getDatabase();
////            client.getTable();
////            client.getColumn();
////            client.createDatabase();
////            client.createTable();
////            client.deleteDatabase();
////            client.deleteTable();
////            client.renameDatabase();
////            client.renameTable();
////            client.renameColumn();
////            client.createFiber();
////            client.listFiberValues();
////            client.addBlockIndex();
////            client.filterBlockPathsByTime();
////            client.filterBlockPaths();
//        } finally {
//            client.shutdown(5);
//        }
//    }
}
