package cn.edu.ruc.iir.paraflow.commons;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;

/**
 * paraflow
 *
 * @author guodong
 */
public class Metric
{
    private final String id;
    private final PushGateway gateway;
    private final Gauge metric;
    private final String job;
    private final CollectorRegistry registry = new CollectorRegistry();

    public Metric(String gateWayUrl, String id, String name, String help, String job)
    {
        this.id = id;
        this.job = job;
        this.gateway = new PushGateway(gateWayUrl);
        this.metric = Gauge.build().name(name).help(help).labelNames("id").register(registry);
    }

    public void addValue(double value)
    {
        metric.labels(id).set(value);
        try {
            gateway.pushAdd(registry, job);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
