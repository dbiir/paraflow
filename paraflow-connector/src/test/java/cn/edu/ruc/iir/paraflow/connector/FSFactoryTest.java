package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.connector
 * @ClassName: FSFactoryTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-07-29 21:31
 **/
public class FSFactoryTest {

    private FSFactory fsFactory = null;
    private final Logger log = Logger.get(FSFactoryTest.class);

    @Before
    public void init ()
    {
        ParaflowPrestoConfig config = new ParaflowPrestoConfig().setParaflowHome("");
        this.fsFactory = new FSFactory(config);
    }

    @Test
    public void testListFiles()
    {
        String hdfsPath = "hdfs://10.77.40.236:9000/metadata/fruit/grip";
        Path path = new Path(hdfsPath);
        List<Path> files = fsFactory.listFiles(path);
        System.out.println(files.toString());
        log.debug("Size: " + files.size());
    }



}
