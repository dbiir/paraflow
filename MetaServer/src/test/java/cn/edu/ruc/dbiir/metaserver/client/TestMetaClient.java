package cn.edu.ruc.dbiir.metaserver.client;

import org.junit.Test;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestMetaClient
{
    @Test
    public void clientSayHiTest()
    {
        MetaClient client = new MetaClient("127.0.0.1", 10012);
        client.sayHi("RPC");
        try {
            client.shutdown(3);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
