package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.collector.DefaultCollector;
import cn.edu.ruc.iir.paraflow.collector.StringMessageSerializationSchema;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.util.Arrays;

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
            if (!collector.existsDatabase("test")) {
                collector.createDatabase("test");
            }
            if (!collector.existsTable("test", "0810")) {
                String[] names = {"id", "number", "name", "creation"};
                String[] types = {"int", "int", "varchar(200)", "bigint"};
                collector.createTable("test", "0810", "parquet", 0, 3,
                        "cn.edu.ruc.iir.paraflow.examples.collector.BasicParaflowFiberPartitioner", Arrays.asList(names), Arrays.asList(types));
            }
            if (!collector.existsTopic("test-0810")) {
                collector.createTopic("test-0810", 10, (short) 1);
            }

            DataSource dataSource = new MockDataSource();
            collector.collect(dataSource, 0, 3,
                    new BasicParaflowFiberPartitioner(),
                    new StringMessageSerializationSchema<>(),
                    new MockDataSink());
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
