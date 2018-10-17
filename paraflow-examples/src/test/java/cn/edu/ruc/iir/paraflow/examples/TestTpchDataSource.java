package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.examples.collector.TpchDataSource;
import org.testng.annotations.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestTpchDataSource
{
    @Test
    public void testTpchDataGeneration()
    {
        TpchDataSource dataSource = new TpchDataSource(1, 1, 1, 0, 1_000_000);
        int counter = 0;
        while (true) {
            Message message = dataSource.read();
            if (message == null) {
                break;
            }
            counter++;
        }
        System.out.println(counter);
    }
}
