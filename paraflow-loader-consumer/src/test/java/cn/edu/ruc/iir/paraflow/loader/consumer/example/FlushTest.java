package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

public class FlushTest
{
//    private TypeDescription schema =
//            TypeDescription.fromString("struct<key:string,ints:array<int>>");
//    private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
//    private OrcList<IntWritable> valueList =
//            (OrcList<IntWritable>) pair.getFieldValue(1);
//    private final NullWritable nada = NullWritable.get();
//
//    public void reduce(Text key, Iterable<IntWritable> values,
//                       Reducer.Context output
//    ) throws IOException, InterruptedException {
//        pair.setFieldValue(0, key);
//        valueList.clear();
//        for(IntWritable val: values) {
//            valueList.add(new IntWritable(val.get()));
//        }
//        output.write(nada, pair);
//    }
    public void flush()
    {
        TypeDescription schema = TypeDescription.createStruct()
                .addField("field1", TypeDescription.createString())
                .addField("field2", TypeDescription.createString())
                .addField("field3", TypeDescription.createString());
        String lxw_orc1_file = "hdfs://ubuntu:9000/test1";
        Configuration conf = new Configuration();
        try {
            FileSystem.getLocal(conf);
            Writer writer = OrcFile.createWriter(new Path(lxw_orc1_file),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema)
                            .stripeSize(67108864)
                            .bufferSize(131072)
                            .blockSize(134217728)
                            .compress(CompressionKind.ZLIB)
                            .version(OrcFile.Version.V_0_12));
            //要写入的内容
            String[] contents = new String[]{"1,a,aa", "2,b,bb", "3,c,cc", "4,d,dd"};

            VectorizedRowBatch batch = schema.createRowBatch();
            for (String content : contents) {
                int rowCount = batch.size++;
                String[] logs = content.split(",", -1);
                for (int i = 0; i < logs.length; i++) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowCount, logs[i].getBytes());
                    //batch full
                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
            }
            writer.addRowBatch(batch);
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String args[]) {
        FlushTest flushTest = new FlushTest();
        flushTest.flush();
    }
}
