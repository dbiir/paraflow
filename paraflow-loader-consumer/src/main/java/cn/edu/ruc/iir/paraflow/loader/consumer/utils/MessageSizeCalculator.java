package cn.edu.ruc.iir.paraflow.loader.consumer.utils;

import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

public class MessageSizeCalculator
{
    private String topic;
    private final MetaClient metaClient;
    private long size = 0;
    public int blockSize;

    public MessageSizeCalculator()
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        this.blockSize = config.getBufferOfferBlockSize();
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public long caculate(String topic)
    {
        this.topic = topic;
        System.out.println("topic :" + topic);
        int dot = topic.indexOf(".");
        int length = topic.length();
        String dbName = topic.substring(0, dot - 1);
        String tblName = topic.substring(dot + 1, length - 1);
        System.out.println("dbName : " + dbName);
        System.out.println("tblName : " + tblName);
        MetaProto.StringListType columnsDataTypeList = metaClient.listColumnsDataType(dbName, tblName);
        System.out.println("columnsDataTypeList : " + columnsDataTypeList);
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
