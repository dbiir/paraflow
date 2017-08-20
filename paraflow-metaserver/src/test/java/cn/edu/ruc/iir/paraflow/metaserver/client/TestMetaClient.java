package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestMetaClient
{
    @Test
    public void clientCreateUserTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createUser("alice", "123456");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createDatabase("food", "hdfs:/127.0.0.1/:5432/metadata/food", "alice");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateDatabase2Test()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createDatabase("food", "alice");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("smell");
        columnName.add("color");
        columnName.add("feel");
        ArrayList<String> columnType = new ArrayList<>();
        columnType.add("regular");
        columnType.add("regular");
        columnType.add("regular");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        MetaProto.StatusType status = client.createTable("food", "rice", 0,
                "alice", "hdfs:/127.0.0.1/:5432/metadata/food/rice",
                1, -1, 1, columnName, columnType, dataType);
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateTable2Test()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("smell");
        columnName.add("color");
        columnName.add("feel");
        ArrayList<String> columnType = new ArrayList<>();
        columnType.add("regular");
        columnType.add("regular");
        columnType.add("regular");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        MetaProto.StatusType status = client.createTable("food", "rice", 0,
                "alice",  1, -1, 1, columnName, columnType, dataType);
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientListDatabasesTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("food").build();
        MetaProto.StringListType stringList = client.listDatabases();
        //System.out.println(stringList);
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientListTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("rice").build();
        MetaProto.StringListType stringList = client.listTables("food");
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientGetDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.DbParam expect = MetaProto.DbParam.newBuilder().setDbName("food").setLocationUrl("hdfs://127.0.0.1:9000/metadata/food").setUserName("alice").build();
        MetaProto.DbParam database = client.getDatabase("food");
        assertEquals(expect, database);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientGetTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        //MetaProto.TblParam expect = MetaProto.TblParam.newBuilder().setDbName("food").setTblName("rice").setTblType(0).setUserName("alice").setCreateTime(1503237074296).setLastAccessTime(1503237074296).setLocationUrl("hdfs://127.0.0.1:9000/metadata/food/rice").setStorageFormatId(1).setFiberColId(-1).setFiberFuncId(1).build();
        MetaProto.TblParam table = client.getTable("food", "rice");
        //assertEquals(expect, table);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientGetColumnTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.ColParam expect = MetaProto.ColParam.newBuilder().setColIndex(0).setTblName("rice").setColName("smell").setColType("regular").setDataType("varchar(20)").build();
        MetaProto.ColParam column = client.getColumn("food", "rice", "smell");
        assertEquals(expect, column);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateDbParamTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createDbParam("food", "She", "is beautiful and smart");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateTblParamTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createTblParam("food", "rice", "It", "is a good book");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateTblPrivTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createTblPriv("food", "rice", "alice", 1);
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateStorageFormatTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createStorageFormat("storageFormatName", "snappy", "serialFormat");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void clientCreateFiberFuncTest()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
//        ByteString fiberFuncContent = ;
//        MetaProto.StatusType status = client.createFiberFunc("fiberFuncName", fiberFuncContent);
//        assertEquals(expect, status);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void clientCreateBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createBlockIndex("food", "rice", 123, 20170814, 20170814, "hdfs:127.0.0.1");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void clientFilterBlockIndexTest()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
//        MetaProto.StringListType stringList = client.filterBlockIndex("default", "book", );
//        assertEquals(expect, stringList);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void clientFilterBlockIndexByFiberTest()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
//        MetaProto.StringListType stringList = client.filterBlockIndexByFiber();
//        assertEquals(expect, stringList);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void clientRenameColumnTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameColumn("food", "rice", "smell", "smellnew");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientRenameTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameTable("food", "rice", "ricenew");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientRenameDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameDatabase("food", "foodnew");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientDeleteTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.deleteTable("foodnew", "ricenew");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientDeleteDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.deleteDatabase("foodnew");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
