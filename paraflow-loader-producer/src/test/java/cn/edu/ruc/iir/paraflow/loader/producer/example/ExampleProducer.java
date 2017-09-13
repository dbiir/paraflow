package cn.edu.ruc.iir.paraflow.loader.producer.example;

import cn.edu.ruc.iir.paraflow.commons.func.SerializableFunction;
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
    private void exampleTest()
    {
        final Producer producer = new DefaultProducer("");
        StatusProto.ResponseStatus userStat = producer.createUser("producer", "123456");
        StatusProto.ResponseStatus dbStat = producer.createDatabase("producerexample",
                "producer",
                "file://127.0.0.1/tmp/producerexample");
        producer.createTopic("example", 100, (short) 1);
        try {
            // todo add an interface for fiber func lambda in paraflow-commons
            SerializableFunction<String, Long> func = (v) -> Long.parseLong(v) % 1000;
            StatusProto.ResponseStatus funStat = producer.createFiberFunc("examplefunc", func);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        List<String> colNames = Arrays.asList("sid", "age", "name", "timestamp");
        List<String> colTypes = Arrays.asList("int", "int", "varchar(20)", "long");
        StatusProto.ResponseStatus tblStat = producer.createFiberTable(
                "producerexample",
                "example",
                "producer",
                "file://127.0.0.1/tmp/producerexample/example",
                "parquet",
                0,
                3,
                "examplefunc",
                colNames,
                colTypes);

        for (int i = 0; i < 10000; i++) {
            long ts = System.currentTimeMillis();
            String[] content = {String.valueOf(i), String.valueOf(i * 2), "alice" + i, String.valueOf(ts)};
            Message msg = new Message(0, content, ts);
            producer.send("producerexample", "example", msg);
        }
    }

    public static void main(String[] args)
    {
        ExampleProducer producer = new ExampleProducer();
        producer.exampleTest();
    }
}
