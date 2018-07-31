package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableLayoutHandle;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowMetaDataReader;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.connector
 * @ClassName: ParaflowMetaDataReaderTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-07-28 21:52
 **/
public class ParaflowMetaDataReaderTest {

    private ParaflowMetaDataReader paraflowMetaDataReader = null;
    private final Logger log = Logger.get(ParaflowMetaDataReaderTest.class);

    @Before
    public void init ()
    {
        ParaflowPrestoConfig config = new ParaflowPrestoConfig().setParaflowHome("");
        this.paraflowMetaDataReader = new ParaflowMetaDataReader(config);
    }

    @Test
    public void testGetAllDatabases()
    {
        List<String> schemaList = paraflowMetaDataReader.getAllDatabases();
        System.out.println(schemaList.toString());
        log.debug("Size: " + schemaList.size());
    }

    @Test
    public void testListTables()
    {
        String prefix = "fruit";
        List<SchemaTableName> tablelist = paraflowMetaDataReader.listTables(prefix);
        System.out.println(tablelist.toString());
        log.debug("Size: " + tablelist.size());
    }

    @Test
    public void testGetTableLayout()
    {
        String connectorId = "paraflow-presto";
        String dbName = "fruit";
        String tblName = "grip";
        Optional<ParaflowTableLayoutHandle> layout = paraflowMetaDataReader.getTableLayout(connectorId, dbName, tblName);
        if(layout.isPresent()){
            ParaflowTableLayoutHandle paraflowTableLayoutHandle = layout.get();
            System.out.println(paraflowTableLayoutHandle.toString());
        }
        System.out.println(layout.isPresent());
    }


}
