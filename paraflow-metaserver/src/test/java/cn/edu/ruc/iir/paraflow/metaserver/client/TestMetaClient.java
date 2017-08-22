package cn.edu.ruc.iir.paraflow.metaserver.client;

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
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createUser(
                "alice",
                "123456");
        assertEquals(expect, status);
    }

    @Test
    public void step02_ClientCreateDatabaseTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createDatabase(
                "food",
                "hdfs://127.0.0.1:5432/metadata/food",
                "alice");
        assertEquals(expect, status);
    }

    @Test
    public void step03_ClientCreateDatabase2Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createDatabase(
                "fruit",
                "alice");
        assertEquals(expect, status);
    }

    // TODO database already exist test

    @Test
    public void step04_ClientCreateStorageFormatTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createStorageFormat(
                "StorageFormatName",
                "snappy",
                "serialFormat");
        assertEquals(expect, status);
    }

    @Test
    public void step05_ClientCreateFiberFuncTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        String string = "I am a girl";
        byte[] fiberFuncContent = string.getBytes();
        MetaProto.StatusType status = client.createFiberFunc(
                "FiberFuncName",
                fiberFuncContent);
        assertEquals(expect, status);
    }

    @Test
    public void step06_ClientCreateRegularTableTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
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
        MetaProto.StatusType status = client.createRegularTable(
                "food",
                "rice",
                "alice",
                "hdfs:/127.0.0.1/:5432/metadata/food/rice",
                "StorageFormatName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step07_ClientCreateRegularTable2Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
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
        MetaProto.StatusType status = client.createRegularTable(
                "food",
                "noodles",
                "alice",
                "StorageFormatName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    // collist size not equal
    public void step08_ClientCreateRegularTable3Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.CREATE_COLUMN_ERROR)
                .build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("smell");
        columnName.add("color");
        ArrayList<String> columnType = new ArrayList<>();
        columnType.add("regular");
        columnType.add("regular");
        columnType.add("regular");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        MetaProto.StatusType status = client.createRegularTable(
                "fruit",
                "apple",
                "alice",
                "StorageFormatName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    // database not found
    public void step09_ClientCreateRegularTable4Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.DATABASE_NOT_FOUND)
                .build();
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
        MetaProto.StatusType status = client.createRegularTable(
                "like",
                "pear",
                "alice",
                "StorageFormatName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    // user not found
    public void step10_ClientCreateRegularTable5Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.USER_NOT_FOUND)
                .build();
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
        MetaProto.StatusType status = client.createRegularTable(
                "fruit",
                "pear",
                "amy",
                "StorageFormatName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    // storage format not found
    public void step11_ClientCreateRegularTable6Test()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.STORAGE_FORMAT_NOT_FOUND)
                .build();
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
        MetaProto.StatusType status = client.createRegularTable(
                "fruit",
                "pear",
                "alice",
                "StorageFormat",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step12_ClientCreateFiberTableTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
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
        MetaProto.StatusType status = client.createFiberTable(
                "fruit",
                "grip",
                "alice",
                "hdfs:/127.0.0.1/:5432/metadata/fruit/grip",
                "StorageFormatName",
                "smell",
                "FiberFuncName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step13_ClientCreateFiberTableTest2()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
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
        MetaProto.StatusType status = client.createFiberTable(
                "fruit",
                "banana",
                "alice",
                "StorageFormatName",
                "smell",
                "FiberFuncName",
                columnName,
                columnType,
                dataType);
        assertEquals(expect, status);
    }

    //TODO table already exist test

    @Test
    public void step14_ClientListDatabasesTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("food")
                .addStr("fruit")
                .build();
        MetaProto.StringListType stringList = client.listDatabases();
        assertEquals(expect, stringList);
    }

    @Test
    public void step15_ClientListTableTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("noodles")
                .addStr("rice")
                .build();
        MetaProto.StringListType stringList = client.listTables("food");
        assertEquals(expect, stringList);
    }

    @Test
    public void step16_ClientGetDatabaseTest()
    {
        MetaProto.DbParam expect = MetaProto.DbParam.newBuilder()
                .setDbName("food")
                .setLocationUrl("hdfs://127.0.0.1:5432/metadata/food")
                .setUserName("alice")
                .build();
        MetaProto.DbParam database = client.getDatabase("food");
        assertEquals(expect, database);
    }

    @Test
    public void step17_ClientGetTableTest()
    {
        //MetaProto.TblParam expect = MetaProto.TblParam.newBuilder().setDbName("food").setTblName("rice").setTblType(0).setUserName("alice").setCreateTime(1503237074296).setLastAccessTime(1503237074296).setLocationUrl("hdfs://127.0.0.1:9000/metadata/food/rice").setStorageFormatId(1).setFiberColId(-1).setFiberFuncId(1).build();
        MetaProto.TblParam table = client.getTable("food", "rice");
        //assertEquals(expect, table);
    }

    @Test
    public void step18_ClientGetColumnTest()
    {
        MetaProto.ColParam expect = MetaProto.ColParam.newBuilder()
                .setColIndex(0)
                .setTblName("rice")
                .setColName("smell")
                .setColType("regular")
                .setDataType("varchar(20)")
                .build();
        MetaProto.ColParam column = client.getColumn(
                "food",
                "rice",
                "smell");
        assertEquals(expect, column);
    }

    @Test
    public void step19_ClientCreateDbParamTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createDbParam(
                "food",
                "She",
                "is beautiful and smart");
        assertEquals(expect, status);
    }

    @Test
    public void step20_ClientCreateTblParamTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createTblParam(
                "food",
                "rice",
                "It",
                "is a good book");
        assertEquals(expect, status);
    }

    @Test
    public void step21_ClientCreateTblPrivTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createTblPriv(
                "food",
                "rice",
                "alice",
                1);
        assertEquals(expect, status);
    }

    @Test
    public void step22_ClientCreateBlockIndexTest1()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createBlockIndex(
                "food",
                "rice",
                123,
                20170800,
                20170802,
                "hdfs://127.0.0.1:5432/metadata/food/rice/123");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step23_ClientCreateBlockIndexTest2()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createBlockIndex(
                "food",
                "rice",
                234,
                20170803,
                20170805,
                "hdfs://127.0.0.1:5432/metadata/food/rice/234");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step24_ClientCreateBlockIndexTest3()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createBlockIndex(
                "food",
                "rice",
                345,
                20170806,
                20170808,
                "hdfs://127.0.0.1:5432/metadata/food/rice/345");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step25_ClientCreateBlockIndexTest4()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder()
                .setStatus(MetaProto.StatusType.State.OK)
                .build();
        MetaProto.StatusType status = client.createBlockIndex(
                "food",
                "rice",
                456,
                20170809,
                20170811,
                "hdfs://127.0.0.1:5432/metadata/food/rice/456");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step26_ClientFilterBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/234")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/345")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/456")
                .build();
        MetaProto.StringListType stringList =
                client.filterBlockIndex(
                        "food",
                        "rice",
                        -1,
                        -1);
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step27_ClientFilterBlockIndexTestBegin()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/234")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/345")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/456")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "food",
                "rice",
                20170804,
                -1);
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step28_ClientFilterBlockIndexTestEnd()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/234")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/345")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "food",
                "rice",
                -1,
                20170807);
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step29_ClientFilterBlockIndexTestBeginEnd()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/234")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/345")
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/456")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "food",
                "rice",
                20170804,
                20170807);
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step30_ClientFilterBlockIndexByFiberTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber(
                "food",
                "rice",
                123,
                -1,
                -1
        );
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step31_ClientFilterBlockIndexByFiberBeginTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber(
                "food",
                "rice",
                123,
                20170800,
                -1
        );
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step32_ClientFilterBlockIndexByFiberEndTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber(
                "food",
                "rice",
                123,
                -1,
                20170802
        );
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step33_ClientFilterBlockIndexByFiberBeginEndTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/metadata/food/rice/123")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber(
                "food",
                "rice",
                123,
                20170800,
                20170802
        );
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step34_Ctep18_ClientRenameColumnTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameColumn("food", "rice", "smell", "smellnew");
        assertEquals(expect, status);
    }

    @Test
    public void step35_ClientRenameTableTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameTable("food", "rice", "ricenew");
        assertEquals(expect, status);
    }

    @Test
    public void step36_ClientRenameDatabaseTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameDatabase("food", "foodnew");
        assertEquals(expect, status);
    }

    @Test
    public void step37_ClientDeleteTableTest()
    {
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.deleteTable("foodnew", "ricenew");
        assertEquals(expect, status);
    }

    @Test
    public void step38_ClientDeleteDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.deleteDatabase("foodnew");
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
