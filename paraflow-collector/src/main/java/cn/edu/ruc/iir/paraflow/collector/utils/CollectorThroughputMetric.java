package cn.edu.ruc.iir.paraflow.collector.utils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;

/**
 * paraflow
 *
 * @author guodong
 */
public class CollectorThroughputMetric
{
    private final CollectorRegistry registry = new CollectorRegistry();
    private final Gauge throughputMetric =
            Gauge.build().name("collector_throughput")
                    .help("Collector throughput (MB/s)")
                    .labelNames("id")
                    .register(registry);
    private final String id;
    private final PushGateway gateway;

    public CollectorThroughputMetric(String id, String gatewayUrl)
    {
        this.id = id;
        this.gateway = new PushGateway(gatewayUrl);
    }

    public void addValue(double value)
    {
        throughputMetric.labels(id).set(value);
        try {
            gateway.pushAdd(registry, "collector_metrics");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
