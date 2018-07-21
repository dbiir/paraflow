package cn.edu.ruc.iir.paraflow.examples.basic;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.producer.CollectorEnvironment;
import cn.edu.ruc.iir.paraflow.loader.producer.CollectorRuntime;
import cn.edu.ruc.iir.paraflow.loader.producer.DataFlow;
import cn.edu.ruc.iir.paraflow.loader.producer.DataSource;
import cn.edu.ruc.iir.paraflow.loader.producer.FiberFlow;

import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class BasicCollector
{
    private CollectorEnvironment env = CollectorEnvironment.getEnvironment();

    public void collectData()
    {
        DataSource<String> dataSource = new MockDataSource();
        DataFlow<String> dataFlow = env.addSource(dataSource);
        FiberFlow<String> fiberFlow = (FiberFlow<String>) dataFlow
                .map(v -> new Message(v.split(",")))
                .keyBy(0)
                .timeBy(0)
                .partitionBy(key -> Objects.hashCode(key) % 10)
                .sink(new MockDataSink());
        CollectorRuntime.run(fiberFlow);
    }

    public static void main(String[] args)
    {
        BasicCollector collector = new BasicCollector();
        collector.collectData();
    }
}
