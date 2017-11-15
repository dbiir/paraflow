package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.FileNameGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParquetFlushThread extends DataFlushThread
{
    private final MetaClient metaClient;
    private Configuration conf = new Configuration();

    public ParquetFlushThread(String threadName)
    {
        super(threadName);
        ConsumerConfig config = ConsumerConfig.INSTANCE();
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
            Path file = new Path(path);
            try {
                String schemaString = "message " + tblName + " {";
                for (int i = 0; i < columnDataTypeCount; i++) {
                    schemaString = schemaString + "required " + columnDataTypeList.getStr(i) + " " + columnsNameList.getStr(i) + "; ";
                }
                schemaString = schemaString + "}";
                MessageType schema = MessageTypeParser.parseMessageType(schemaString);
                GroupFactory factory = new SimpleGroupFactory(schema);
                Group group = factory.newGroup();
                for (String[] contents = segment.getNext(); segment.hasNext(); contents = segment.getNext()) {
                    for (int i = 0; i < contents.length; i++) {
                        switch (columnDataTypeList.getStr(i)) {
                            case "bigint":
                                group.append(columnsNameList.getStr(i), Long.parseLong(contents[i]));
                                break;
                            case "int":
                                group.append(columnsNameList.getStr(i), Integer.parseInt(contents[i]));
                                break;
                            case "boolean":
                                group.append(columnsNameList.getStr(i), Boolean.parseBoolean(contents[i]));
                                break;
                            case "float4":
                                group.append(columnsNameList.getStr(i), Float.parseFloat(contents[i]));
                                break;
                            case "float8":
                                group.append(columnsNameList.getStr(i), Double.parseDouble(contents[i]));
                                break;
                            case "timestamptz":
                                group.append(columnsNameList.getStr(i), Long.parseLong(contents[i]));
                                break;
                            case "real" :
                                group.append(columnsNameList.getStr(i), Float.parseFloat(contents[i]));
                                break;
                            default:
                                group.append(columnsNameList.getStr(i), contents[i]);
                                break;
                        }
                    }
                }
                GroupWriteSupport writeSupport = new GroupWriteSupport();
                writeSupport.setSchema(schema, conf);
                ParquetWriter<Group> writer = new ParquetWriter<Group>(file, writeSupport,
                        ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                        ParquetProperties.WriterVersion.PARQUET_1_0,
                        conf
                );
                writer.write(group);
                writer.close();
                segment.setFilePath(path);
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
