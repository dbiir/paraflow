package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.HostAddress;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.connector
 * @ClassName: TestHdfsFile
 * @Description:
 * @author: tao
 * @date: Create in 2019-09-14 18:50
 **/
public class TestHdfsFile
{
    ParaflowPrestoConfig config;
    FSFactory fsFactory;

    @Before
    public void setUp()
    {
        config = new ParaflowPrestoConfig();
        config.setHDFSWarehouse("hdfs://dbiir01:9000/paraflow");
        config.setMetaserverHost("127.0.0.1");
        config.setMetaserverPort(10012);
        fsFactory = new FSFactory(config);
    }

    @Test
    public void testFileLocation()
    {
        Path tablePath = new Path("hdfs://dbiir01:9000/paraflow/test/tpch/019355364443918028120303902");
        List<HostAddress> addresses = fsFactory.getBlockLocations(tablePath, 0, Long.MAX_VALUE);
        for (HostAddress s : addresses) {
            System.out.println(s.getHostText());
        }
    }
}
