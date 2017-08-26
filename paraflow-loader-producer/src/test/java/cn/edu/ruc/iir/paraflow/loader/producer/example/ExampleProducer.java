package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.producer.DefaultProducer;
import cn.edu.ruc.iir.paraflow.loader.producer.Producer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class ExampleProducer
{
    public static void main(String[] args)
    {
        final Producer producer = new DefaultProducer();
        StatusProto.ResponseStatus userStat = producer.createUser("producer", "123456");
        StatusProto.ResponseStatus dbStat = producer.createDatabase("producerexample",
                "producer",
                "file://127.0.0.1/tmp/producerexample");
        producer.createTopic("example", 100, (short) 1);
        try
        {
            StatusProto.ResponseStatus funStat = producer.createFiberFunc("examplefunc", mid -> {
                return (int) (mid % 1000);
            });
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        List<String> colNames = Arrays.asList("sid", "age", "name");
        List<String> colTypes = Arrays.asList("int", "int", "varchar(20)");
        StatusProto.ResponseStatus tblStat = producer.createFiberTable("producerexample",
                "example",
                "producer",
                "file://127.0.0.1/tmp/producerexample/example",
                "parquet",
                "sid",
                "examplefunc",
                colNames,
                colTypes);

        for (int i = 0; i < 10000; i++) {
            String content = "hello" + i;
            Message msg = new Message((long) i, content.getBytes(), System.currentTimeMillis());
            producer.send("producerexample", "example", msg);
        }
    }
}
