package cn.edu.ruc.iir.paraflow.loader.utils;

import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

public class MessageSizeCalculator
{
    private static ConsumerConfig config = ConsumerConfig.INSTANCE();
    public static MetaClient metaClient
            = new MetaClient(config.getMetaServerHost(), config.getMetaServerPort());

    private MessageSizeCalculator()
    {
    }

    public static long caculate(String topic)
    {
        long size = 0;
        int dot = topic.indexOf(".");
        int length = topic.length();
        String dbName = topic.substring(0, dot);
        String tblName = topic.substring(dot + 1, length);
        MetaProto.StringListType columnsDataTypeList = metaClient.listColumnsDataType(dbName, tblName);
        int stringCount = columnsDataTypeList.getStrCount();
        for (int i = 0; i < stringCount; i++) {
            switch (columnsDataTypeList.getStr(i)) {
                case "bigint":
                    size += 8;
                    break;
                case "int":
                    size += 4;
                    break;
                case "boolean":
                    size += 1;
                    break;
                default:
                    int indexOfRP = columnsDataTypeList.getStr(i).indexOf(")");
                    int varCharSize = Integer.parseInt(columnsDataTypeList.getStr(i).substring(8, indexOfRP));
                    size += varCharSize;
            }
        }
        return size;
    }
}
