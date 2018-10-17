package cn.edu.ruc.iir.paraflow.collector;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestKafkaBasic
{
    @Test
    public void createTopic()
    {
        try {
            DefaultCollector<String> collector = new DefaultCollector<>();
            collector.createTopic("exampledb-exampletbl", 10, (short) 1);
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
