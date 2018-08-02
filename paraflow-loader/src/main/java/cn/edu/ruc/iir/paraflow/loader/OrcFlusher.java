package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.loader.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.utils.FileNameGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

/**
 * paraflow
 *
 * @author guodong
 */
public class OrcFlusher extends DataFlusher
{
    private long orcFileStripeSize;
    private int orcFileBufferSize;
    private long orcFileBlockSize;

    public OrcFlusher(String threadName)
    {
        super(threadName);
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        this.orcFileStripeSize = config.getOrcFileStripeSize();
        this.orcFileBufferSize = config.getOrcFileBufferSize();
        this.orcFileBlockSize = config.getOrcFileBlockSize();
    }

    @Override
    boolean flushData(BufferSegment segment)
    {
        System.out.println("Orc file flush out!!!");
        String topic = segment.getFiberPartitions().get(0).getTopic();
        int indexOfDot = topic.indexOf(".");
        String dbName = topic.substring(0, indexOfDot);
        int length = topic.length();
        String tblName = topic.substring(indexOfDot + 1, length);

        long beginTime = segment.getTimestamps()[0];
        long endTime = segment.getTimestamps()[0];
        for (long timeStamp : segment.getTimestamps()) {
            if (timeStamp < beginTime) {
                beginTime = timeStamp;
            }
            if (timeStamp > endTime) {
                endTime = timeStamp;
            }
        }
        String path = FileNameGenerator.generator(dbName, tblName, beginTime, endTime);

        MetaProto.StringListType columnsNameList = metaClient.listColumns(dbName, tblName);
        MetaProto.StringListType columnDataTypeList = metaClient.listColumnsDataType(dbName, tblName);
        int columnNameCount = columnsNameList.getStrCount();
        int columnDataTypeCount = columnDataTypeList.getStrCount();
        System.out.println("columnNameCount: " + columnNameCount);
        System.out.println("columnDataTypeCount: " + columnDataTypeCount);

        TypeDescription schema = TypeDescription.createStruct();
        if (columnNameCount == columnDataTypeCount) {
            for (int i = 0; i < columnNameCount; i++) {
                switch (columnDataTypeList.getStr(i)) {
                    case "bigint":
                        System.out.println("bigint");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createLong());
                        break;
                    case "int":
                        System.out.println("int");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createInt());
                        break;
                    case "boolean":
                        System.out.println("boolean");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createBoolean());
                        break;
                    case "float4":
                        System.out.println("float4");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createFloat());
                        break;
                    case "float8":
                        System.out.println("float8");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createDouble());
                        break;
                    case "timestamptz":
                        System.out.println("timestamptz");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createTimestamp());
                        break;
                    case "real":
                        System.out.println("real");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createFloat());
                        break;
                    default:
                        System.out.println("default");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createString());
                        break;
                }
            }
        }

        return flush(segment, path, schema);
    }

    private boolean flush(BufferSegment segment, String path, TypeDescription schema)
    {
        Configuration conf = new Configuration();
        try {
            Writer writer = OrcFile.createWriter(new Path(path),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema)
                            .stripeSize(orcFileStripeSize)
                            .bufferSize(orcFileBufferSize)
                            .blockSize(orcFileBlockSize)
                            .compress(CompressionKind.ZLIB)
                            .version(OrcFile.Version.V_0_12));
            VectorizedRowBatch batch = schema.createRowBatch();
            while (segment.hasNext()) {
                String[] contents = segment.getNext();
                int rowCount = batch.size++;
//                    System.out.println("contents : message.getValues() : " + Arrays.toString(contents));
                System.out.println("contents.length : " + contents.length);
                for (int i = 0; i < contents.length; i++) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowCount, contents[i].getBytes());
                    //batch full
                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                if (batch.size != 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
                writer.close();
                segment.setFilePath(path);
                System.out.println("path : " + path);
            }
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
