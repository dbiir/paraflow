package cn.edu.ruc.iir.paraflow.metaserver.client;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.metaserver.client
 * @ClassName: TestMetadata
 * @Description: To generate a table and columns
 * @author: tao
 * @date: Create in 2018-08-01 10:47
 **/
public class TestMetadata
{
    MetaClient client;

    @Before
    public void init()
    {
        client = new MetaClient("127.0.0.1", 10012);
    }

    @Test
    public void step06_ClientCreateRegularTableTest()
    {
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        ArrayList<String> columnName = new ArrayList<>();
        columnName.add("age");
        columnName.add("sex");
        columnName.add("name");
        ArrayList<String> columnType = new ArrayList<>();
        columnType.add("regular");
        columnType.add("regular");
        columnType.add("regular");
        ArrayList<String> dataType = new ArrayList<>();
        dataType.add("int");
        dataType.add("boolean");
        dataType.add("varchar(10)");
        StatusProto.ResponseStatus status = client.createRegularTable("fruit", "alltype",
                "paraflow", "hdfs://10.77.40.27:9000/metadata/fruit",
                "StorageFormatName", columnName, dataType);
        assertEquals(expect, status);
    }
}
