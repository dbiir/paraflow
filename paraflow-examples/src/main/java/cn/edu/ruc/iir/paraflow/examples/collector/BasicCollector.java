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
            if (!collector.existsTable("test", "tbl0810")) {
                String[] names = {"id", "number", "name", "creation"};
                String[] types = {"integer", "integer", "varchar(200)", "bigint"};
                collector.createTable("test", "tbl0810", "parquet", 0, 3,
                        "cn.edu.ruc.iir.paraflow.examples.collector.BasicParaflowFiberPartitioner",
                        Arrays.asList(names), Arrays.asList(types));
            }
            if (!collector.existsTopic("test-tbl0810")) {
                collector.createTopic("test-tbl0810", 10, (short) 1);
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
