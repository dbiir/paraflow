package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.FileNameGenerator;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.loader.consumer.threads
 * @ClassName: ParquetFlushThreadTest
 * @Description: Parquest file
 * @author: tao
 * @date: Create in 2018-07-26 10:41
 **/
public class ParquetFlushThreadTest extends DataFlushThread
{
    private Configuration conf = new Configuration();

    public ParquetFlushThreadTest(String threadName, String configPath)
    {
        super(threadName, configPath);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public void genFileTest()
    {
        System.out.println("Parquet file flush out!!!");
        String dbName = "fruit";
        String tblName = "grip";
        long beginTime = 1532313547665L;
        long endTime = 1532313547665L;

        String path = FileNameGenerator.generator(dbName, tblName, beginTime, endTime);
        MetaProto.StringListType columnsNameList = metaClient.listColumns(dbName, tblName);
        MetaProto.StringListType columnDataTypeList = metaClient.listColumnsDataType(dbName, tblName);
        int columnNameCount = columnsNameList.getStrCount();
        int columnDataTypeCount = columnDataTypeList.getStrCount();
        System.out.println("Path: " + path);
        if (columnNameCount == columnDataTypeCount) {
            Path file = new Path(path);
            try {
                StringBuilder schemaString = new StringBuilder("message " + tblName + " {");
                for (int i = 0; i < columnDataTypeCount; i++) {
                    switch (columnDataTypeList.getStr(i)) {
                        case "bigint":
                            schemaString.append("required INT64 ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "int":
                            schemaString.append("required INT32 ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "boolean":
                            schemaString.append("required BOOLEAN ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "float4":
                            schemaString.append("required FLOAT ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "float8":
                            schemaString.append("required DOUBLE ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "timestamptz":
                            schemaString.append("required INT64 ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        case "real":
                            schemaString.append("required INT64 ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                        default:
                            schemaString.append("required BINARY ").append(columnsNameList.getStr(i)).append("; ");
                            break;
                    }
                }
                schemaString.append("}");
                System.out.println("Parquet file schema: " + schemaString);
                MessageType schema = MessageTypeParser.parseMessageType(schemaString.toString());
                GroupFactory factory = new SimpleGroupFactory(schema);
                GroupWriteSupport writeSupport = new GroupWriteSupport();
                GroupWriteSupport.setSchema(schema, conf);
                ParquetWriter<Group> writer = new ParquetWriter<Group>(file, writeSupport,
                        ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        false,
                        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                        ParquetProperties.WriterVersion.PARQUET_2_0,
                        conf
                );
                for (int j = 0; j < 100000000; j++) {
                    String value = "4,5,6";
                    String[] contents = value.split(",");
                    Group group = factory.newGroup();
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
                            case "real":
                                group.append(columnsNameList.getStr(i), Float.parseFloat(contents[i]));
                                break;
                            default:
                                group.append(columnsNameList.getStr(i), contents[i]);
                                break;
                        }
                    }
                    writer.write(group);
                }
                writer.close();
            }
            catch (IOException e) {
                e.printStackTrace();
                System.out.println("Error");
            }
        }
        else {
            System.out.println("Error");
        }
    }

    @Override
    boolean flushData(BufferSegment segment)
    {
        genFileTest();
        return true;
    }

    public static void main(String[] args)
    {
        if (args.length == 0) {
            System.out.println("Default args: /home/tao/software/station/DBIIR/paraflow/dist/conf/metaserver.conf");
            args = new String[]{"/home/tao/software/station/DBIIR/paraflow/dist/conf/loader-consumer.conf"};
        }
        ParquetFlushThreadTest parquetFlushThreadTest = new ParquetFlushThreadTest("Test", args[0]);
        parquetFlushThreadTest.flushData(null);
    }
}
