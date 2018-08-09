package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.collector.DefaultCollector;
import cn.edu.ruc.iir.paraflow.collector.StringMessageSerializationSchema;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

/**
 * paraflow
 *
 * @author guodong
 */
public class BasicCollector
{
    private BasicCollector()
    {}

    public static void main(String[] args)
    {
        try {
            DefaultCollector<String> collector = new DefaultCollector<>();
            for (int i = 0; i < 1; i++) {
                DataSource dataSource = new MockDataSource();
                collector.collect(dataSource, 0, 0,
                        new BasicParaflowFiberPartitioner(),
                        new StringMessageSerializationSchema<>(),
                        new MockDataSink());
            }
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
