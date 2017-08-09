package cn.edu.ruc.iir.paraflow.metaserver.client;

import org.junit.Before;
import org.junit.Test;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
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
        assertEquals(expect,stringList);
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
        assertEquals(expect,stringList);
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
        MetaProto.DbModel expect= MetaProto.DbModel.newBuilder().setDbName("default").setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default").setUserId(1).build();
        MetaProto.DbModel database = client.getDatabase();
        assertEquals(expect,database);
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
        MetaProto.TblModel expect = MetaProto.TblModel.newBuilder().setDbId(1).setCreateTime(20170807).setLastAccessTime(20170807).setUserId(1).setTblName("employee").setTblType(0).setFiberColId(-1).setLocationUrl("hdfs:/127.0.0.1:9000/warehouse/default/employee").setStorageFormat(1).setFiberFuncId(1).build();
        MetaProto.TblModel table = client.getTable();
        assertEquals(expect,table);
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
        MetaProto.ColModel expect = MetaProto.ColModel.newBuilder().setTblId(1).setColName("name").setColType("regular").setDataType("varchar(20)").setColIndex(0).build();
        MetaProto.ColModel column = client.getColumn();
        assertEquals(expect,column);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
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
        assertEquals(expect,status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientCreateFiberTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.createFiber();
        assertEquals(expect,status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientListFiberValuesTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.LongListType expect = MetaProto.LongListType.newBuilder().addLon(123456).addLon(234567).addLon(345678).build();
        MetaProto.LongListType longList = client.listFiberValues();
        assertEquals(expect,longList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientAddBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StatusType expect = MetaProto.StatusType.newBuilder().setStatus(MetaProto.StatusType.State.OK).build();
        MetaProto.StatusType status = client.addBlockIndex();
        assertEquals(expect,status);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientFilterBlockPathsByTimeTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr(" hdfs://127.0.0.1:9000/warehouse/default/employee/20170807123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/20170807234567").build();
        MetaProto.StringListType stringList = client.filterBlockPathsByTime();
        assertEquals(expect,stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clientFilterBlockPathsTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        MetaProto.StringListType expect = MetaProto.StringListType.newBuilder().addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/123456").addStr("hdfs://127.0.0.1:9000/warehouse/default/employee/234567").build();
        MetaProto.StringListType stringList = client.filterBlockPaths();
        assertEquals(expect,stringList);
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
