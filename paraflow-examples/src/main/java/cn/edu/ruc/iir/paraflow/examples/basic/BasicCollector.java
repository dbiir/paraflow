package cn.edu.ruc.iir.paraflow.examples.basic;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.collector.DefaultCollector;
import cn.edu.ruc.iir.paraflow.collector.StringMessageSerializationSchema;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.util.Objects;

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
            DefaultCollector<String> collector = new DefaultCollector<>(args[0]);
            for (int i = 0; i < 1; i++) {
                DataSource dataSource = new MockDataSource();
                collector.collect(dataSource, 0, 0,
                        k -> Objects.hashCode(k) % 10,
                        new StringMessageSerializationSchema<>(),
                        new MockDataSink());
            }
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
