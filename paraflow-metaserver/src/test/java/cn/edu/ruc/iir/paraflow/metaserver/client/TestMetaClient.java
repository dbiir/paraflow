package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestMetaClient
{
    @Test
    public void clientListDatabasesTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("test").addStr("default").build();
        MetaProto.StringListType stringList = client.listDatabases();
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
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("employee").addStr("student").addStr("book").build();
        MetaProto.StringListType stringList = client.listTables();
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
        MetaProto.DbParam expect = MetaProto.DbParam.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserName("Alice").build();
        MetaProto.DbParam database = client.getDatabase();
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
        MetaProto.TblParam expect = MetaProto.TblParam.newBuilder().setDbName("default").setTblName("employee").setTblType(0).setUserName("Alice").setCreateTime(20170807).setLastAccessTime(20170807).setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default/employee").setStorageFormatId(1).setTblType(0).setFiberColId(-1).setFiberFuncId(1).build();
        MetaProto.TblParam table = client.getTable();
        assertEquals(expect, table);
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
        MetaProto.ColParam expect = MetaProto.ColParam.newBuilder().setColIndex(0).setTblName("employee").setColName("name").setColType("regular").setDataType("varchar(20)").build();
        MetaProto.ColParam column = client.getColumn();
        assertEquals(expect, column);
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
        MetaProto.StatusType status = client.createDatabase();
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
        MetaProto.StatusType status = client.createTable();
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
        MetaProto.StatusType status = client.deleteDatabase();
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
        MetaProto.StatusType status = client.deleteTable();
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
        MetaProto.StatusType status = client.renameDatabase();
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
        MetaProto.StatusType status = client.renameTable();
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientRenameColumnTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.renameColumn();
        assertEquals(expect, status);
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
        MetaProto.StatusType status = client.createDbParam();
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
        MetaProto.StatusType status = client.createTblParam();
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
        MetaProto.StatusType status = client.createTblPriv();
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
        MetaProto.StatusType status = client.createStorageFormat();
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateFiberFuncTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createFiberFunc();
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void clientCreateBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createBlockIndex();
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateUserTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createUser();
        assertEquals(expect, status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientFilterBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
        MetaProto.StringListType stringList = client.filterBlockIndex();
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientFilterBlockIndexByFiberTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
        MetaProto.StringListType stringList = client.filterBlockIndexByFiber();
        assertEquals(expect, stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
