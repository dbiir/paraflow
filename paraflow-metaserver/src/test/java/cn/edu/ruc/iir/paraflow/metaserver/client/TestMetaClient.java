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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMetaClient
{
    private MetaClient client;

    @Before
    public void init()
    {
        client = new MetaClient("10.77.110.27", 10012);
    }

    @Test
    // User is not found. Please create the user first.
    public void step01_ClientCreateUserTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createUser("paraflow", "paraflow");
        assertEquals(expect, status);
    }

    @Test
    public void step02_ClientCreateDatabaseTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createDatabase("test", "hdfs://127.0.0.1:5432/paraflow/test", "paraflow");
        assertEquals(expect, status);
    }

    @Test
    public void step03_ClientCreateDatabase2Test()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.createDatabase("regular", "paraflow");
        assertEquals(expect, status);
    }

    @Test
    public void step07_ClientCreateRegularTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("smell");
        columnName.add("color");
        columnName.add("feel");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("varchar(20)");
        dataType.add("int");
        dataType.add("varchar(20)");
        StatusProto.ResponseStatus status = client.createTable("regular", "noodles", "text", columnName, dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step08_ClientCreateFiberTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("id");
        columnName.add("color");
        columnName.add("feel");
        columnName.add("creation");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("int");
        dataType.add("varchar(20)");
        dataType.add("varchar(20)");
        dataType.add("bigint");
        StatusProto.ResponseStatus status = client.createTable("test", "grip",
                "paraflow", "StorageFormatName", 0,
                "cn.edu.ruc.iir.paraflow.examples.collector.BasicParaflowFiberPartitioner", 3, columnName, dataType);
        assertEquals(expect, status);
    }

    @Test
    public void step10_ClientListDatabasesTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("test").addStr("regular").build();
        MetaProto.StringListType stringList = client.listDatabases();
        assertEquals(expect, stringList);
    }

    // no test
    @Test
    public void step11_ClientListTableTest()
    {
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("grip").build();
        MetaProto.StringListType stringList = client.listTables("test");
        assertEquals(expect, stringList);
    }

    @Test
    public void step18_ClientCreateBlockIndexTest1()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
                .build();
        StatusProto.ResponseStatus status = client.createBlockIndex(
                "test",
                "grip",
                123,
                20170800,
                20170802,
                "hdfs://127.0.0.1:5432/paraflow/test/grip/123");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step19_ClientCreateBlockIndexTest2()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
                .build();
        StatusProto.ResponseStatus status = client.createBlockIndex(
                "test",
                "grip",
                234,
                20170803,
                20170805,
                "hdfs://127.0.0.1:5432/paraflow/test/grip/234");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step20_ClientCreateBlockIndexTest3()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
                .build();
        StatusProto.ResponseStatus status = client.createBlockIndex(
                "test",
                "grip",
                345,
                20170806,
                20170808,
                "hdfs://127.0.0.1:5432/paraflow/test/grip/345");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step21_ClientCreateBlockIndexTest4()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.STATUS_OK)
                .build();
        StatusProto.ResponseStatus status = client.createBlockIndex(
                "test",
                "grip",
                456,
                20170809,
                20170811,
                "hdfs://127.0.0.1:5432/paraflow/test/grip/456");
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void step22_ClientFilterBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/123")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/234")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/345")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/456")
                .build();
        MetaProto.StringListType stringList =
                client.filterBlockIndex(
                        "test",
                        "grip",
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
    public void step23_ClientFilterBlockIndexTestBegin()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/234")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/345")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/456")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "test",
                "grip",
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
    public void step24_ClientFilterBlockIndexTestEnd()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/123")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/234")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/345")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "test",
                "grip",
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
    public void step25_ClientFilterBlockIndexTestBeginEnd()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/123")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/234")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/345")
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/456")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndex(
                "test",
                "grip",
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
    public void step29_ClientFilterBlockIndexByFiberBeginEndTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder()
                .addStr("hdfs://127.0.0.1:5432/paraflow/test/grip/123")
                .build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber(
                "test",
                "grip",
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

    // todo add update block path tests

    @Test
    public void step33_ClientDeleteTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.deleteTable("regular", "noodles");
        assertEquals(expect, status);
    }

    @Test
    public void step34_ClientDeleteDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.deleteDatabase("regular");
        assertEquals(expect, status);
    }

    @Test
    public void step35_ClientDeleteDatabase2Test()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status = client.deleteDatabase("test");
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
