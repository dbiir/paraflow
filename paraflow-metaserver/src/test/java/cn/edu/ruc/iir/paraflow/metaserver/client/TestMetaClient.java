package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMetaClient
{
    MetaClient client;

    @Before
    public void init()
    {
        client = new MetaClient("127.0.0.1", 10012);
    }

    @Test
    public void step01_ClientCreateUserTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createUser("alice", "123456");
        assertEquals(expect, status);
    }

    @Test
    public void step02_ClientCreateDatabaseTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createDatabase("food", "hdfs://127.0.0.1:5432/metadata/food", "alice");
        assertEquals(expect, status);
    }

    @Test
    public void step03_ClientCreateDatabase2Test()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createDatabase("fruit", "alice");
        assertEquals(expect, status);
    }

    // TODO database already exist test

    @Test
    public void step04_ClientCreateStorageFormatTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createStorageFormat("StorageFormatName", "snappy", "serialFormat");
        assertEquals(expect, status);
    }

    @Test
    public void step05_ClientCreateFiberFuncTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        String string = "I am a girl";
        byte[] fiberFuncContent = string.getBytes();
        StatusProto.ResponseStatus status = client.createFiberFunc("FiberFuncName", fiberFuncContent);
        assertEquals(expect, status);
    }

    @Test
    public void step06_ClientCreateRegularTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
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
        StatusProto.ResponseStatus status = client.createRegularTable("food", "rice",
                "alice", "hdfs:/127.0.0.1/:5432/metadata/food/rice",
                "StorageFormatName", columnName, columnType, dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step07_ClientCreateRegularTable2Test()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
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
        StatusProto.ResponseStatus status = client.createRegularTable("food", "noodles",
                "alice", "StorageFormatName", columnName, columnType, dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step08_ClientCreateFiberTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
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
        StatusProto.ResponseStatus status = client.createFiberTable("fruit", "grip",
                "alice", "StorageFormatName",
                "smell", "FiberFuncName", columnName, columnType, dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step09_ClientCreateFiberTableTest2()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
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
        StatusProto.ResponseStatus status = client.createFiberTable("fruit", "banana",
                "alice", "StorageFormatName", "smell", "FiberFuncName", columnName, columnType, dataType);
        assertEquals(expect, status);
    }

    //TODO table already exist test

    @Test
    public void step10_ClientListDatabasesTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("food").addStr("fruit").build();
        MetaProto.StringListType stringList = client.listDatabases();
        assertEquals(expect, stringList);
    }

    @Test
    public void step11_ClientListTableTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("noodles").addStr("rice").build();
        MetaProto.StringListType stringList = client.listTables("food");
        assertEquals(expect, stringList);
    }

    @Test
    public void step12_ClientGetDatabaseTest()
    {
        MetaProto.DbParam expect = MetaProto.DbParam.newBuilder().setDbName("food").setLocationUrl("hdfs://127.0.0.1:5432/metadata/food").setUserName("alice").build();
        MetaProto.DbParam database = client.getDatabase("food");
        assertEquals(expect, database);
    }

    @Test
    public void step13_ClientGetTableTest()
    {
        //MetaProto.TblParam expect = MetaProto.TblParam.newBuilder().setDbName("food").setTblName("rice").setTblType(0).setUserName("alice").setCreateTime(1503237074296).setLastAccessTime(1503237074296).setLocationUrl("hdfs://127.0.0.1:9000/metadata/food/rice").setStorageFormatId(1).setFiberColId(-1).setFiberFuncId(1).build();
        MetaProto.TblParam table = client.getTable("food", "rice");
        //assertEquals(expect, table);
    }

    @Test
    public void step14_ClientGetColumnTest()
    {
        MetaProto.ColParam expect = MetaProto.ColParam.newBuilder().setColIndex(0).setTblName("rice").setColName("smell").setColType("regular").setDataType("varchar(20)").build();
        MetaProto.ColParam column = client.getColumn("food", "rice", "smell");
        assertEquals(expect, column);
    }

    @Test
    public void step15_ClientCreateDbParamTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createDbParam("food", "She", "is beautiful and smart");
        assertEquals(expect, status);
    }

    @Test
    public void step16_ClientCreateTblParamTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createTblParam("food", "rice", "It", "is a good book");
        assertEquals(expect, status);
    }

    @Test
    public void step17_ClientCreateTblPrivTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createTblPriv("food", "rice", "alice", 1);
        assertEquals(expect, status);
    }

//    @Test
//    public void clientCreateBlockIndexTest1()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
//        StatusProto.ResponseStatus status = client.createBlockIndex("food", "rice", 123, 20170814, 20170814, "hdfs:127.0.0.1");
//        assertEquals(expect, status);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void clientCreateBlockIndexTest2()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
//        StatusProto.ResponseStatus status = client.createBlockIndex("food", "rice", 234, 20170814, 20170814, "hdfs:127.0.0.1");
//        assertEquals(expect, status);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void clientCreateBlockIndexTest3()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
//        StatusProto.ResponseStatus status = client.createBlockIndex("food", "rice", 345, 20170814, 20170814, "hdfs:127.0.0.1");
//        assertEquals(expect, status);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void clientCreateBlockIndexTest4()
//    {
//        MetaClient client = new MetaClient("127.0.0.1", 10012);
//        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
//        StatusProto.ResponseStatus status = client.createBlockIndex("food", "rice", 456, 20170814, 20170814, "hdfs:127.0.0.1");
//        assertEquals(expect, status);
//        try {
//            client.shutdown(3);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
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
//    public void clientFilterBlockIndexTestBegin()
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
//    public void clientFilterBlockIndexTestEnd()
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
//    public void clientFilterBlockIndexTestBeginEnd()
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
    public void step18_ClientRenameColumnTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.renameColumn("food", "rice", "smell", "smellnew");
        assertEquals(expect, status);
    }

    @Test
    public void step19_ClientRenameTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.renameTable("food", "rice", "ricenew");
        assertEquals(expect, status);
    }

    @Test
    public void step20_ClientRenameDatabaseTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.renameDatabase("food", "foodnew");
        assertEquals(expect, status);
    }

    @Test
    public void step21_ClientDeleteTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.deleteTable("foodnew", "ricenew");
        assertEquals(expect, status);
    }

    @Test
    public void step22_ClientDeleteDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.deleteDatabase("foodnew");
        assertEquals(expect, status);
    }

    @After
    public void close()
    {
        try {
            client.shutdown(5);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
