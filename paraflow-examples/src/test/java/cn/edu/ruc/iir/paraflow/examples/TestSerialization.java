package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.examples.collector.TpchDataSource;
import cn.edu.ruc.iir.paraflow.examples.loader.TpchDataTransformer;
import org.testng.annotations.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestSerialization
{
    @Test
    public void testSerDe()
    {
        TpchDataSource dataSource = new TpchDataSource(1, 1, 1, 0, 1_000_000);
        TpchDataTransformer transformer = new TpchDataTransformer();

        Message message = dataSource.read();
        if (message != null) {
            byte[] serialized = message.getValue();
            ParaflowRecord record = transformer.transform(serialized, 0);
            assert message.getTimestamp() == record.getTimestamp();
        }
    }
}
