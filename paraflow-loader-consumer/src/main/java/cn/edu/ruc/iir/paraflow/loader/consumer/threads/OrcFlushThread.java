package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.FileNameGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
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

/**
 * paraflow
 *
 * @author guodong
 */
public class OrcFlushThread extends DataFlushThread
{
    private final MetaClient metaClient;
    private long orcFileStripeSize;
    private int orcFileBufferSize;
    private long orcFileBlockSize;
    private String hdfsWarehouse;

    public OrcFlushThread(String threadName)
    {
        super(threadName);
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        this.orcFileStripeSize = config.getOrcFileStripeSize();
        this.orcFileBufferSize = config.getOrcFileBufferSize();
        this.orcFileBlockSize = config.getOrcFileBlockSize();
        this.hdfsWarehouse = config.getHDFSWarehouse();
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
    }

    @Override
    boolean flushData(BufferSegment segment)
    {
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
        System.out.println("columnNameCount : " + columnNameCount);
        System.out.println("columnDataTypeCount : " + columnDataTypeCount);
        if (columnNameCount == columnDataTypeCount) {
            TypeDescription schema = TypeDescription.createStruct();
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
                    case "float8":
                        System.out.println("float8");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createDouble());
                    case "timestamptz":
                        System.out.println("timestamptz");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createTimestamp());
                    case "real" :
                        System.out.println("real");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createFloat());
                    default:
                        System.out.println("default");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createString());
                }
            }
            Configuration conf = new Configuration();
            try {
                FileSystem.getLocal(conf);
                Writer writer = OrcFile.createWriter(new Path(path),
                        OrcFile.writerOptions(conf)
                                .setSchema(schema)
                                .stripeSize(orcFileStripeSize)
                                .bufferSize(orcFileBufferSize)
                                .blockSize(orcFileBlockSize)
                                .compress(CompressionKind.ZLIB)
                                .version(OrcFile.Version.V_0_12));
                VectorizedRowBatch batch = schema.createRowBatch();
                for (String[] contents = segment.getNext(); segment.hasNext(); contents = segment.getNext()) {
                        int rowCount = batch.size++;
                        System.out.println("contents : message.getValues() : " + contents);
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
        else {
            return false;
        }
    }
}
