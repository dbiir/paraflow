package cn.edu.ruc.iir.paraflow.metaserver.client;

import org.junit.Test;

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
        client.listDatabases();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientGetDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.getDatabase();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientGetTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.getTable();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientGetColumnTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.getColumn();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientCreateDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.createDatabase();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientCreateTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.createTable();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientDeleteDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.deleteDatabase();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientDeleteTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.deleteTable();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientRenameDatabaseTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.renameDatabase();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientRenameTableTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.renameTable();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientRenameColumnTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.renameColumn();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientCreateFiberTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.createFiber();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientListFiberValuesTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.listFiberValues();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientAddBlockIndexTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.addBlockIndex();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientFilterBlockPathsByTimeTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.filterBlockPathsByTime();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    @Test
    public void clientFilterBlockPathsTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.filterBlockPaths();
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
