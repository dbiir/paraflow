package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageListComparator;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageSizeCalculator;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class DataProcessThread implements Runnable
{
    private final String threadName;
    private boolean isReadyToStop = false;
    private final MetaClient metaClient;
    private LinkedList<Message> messages = new LinkedList<>();
    private Map<Integer, LinkedList<Message>> messageLists = new HashMap<>();
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private String hdfsWarehouse;
    private String dbName;
    private String tblName;
    private long orcFileStripeSize;
    private int orcFileBufferSize;
    private long orcFileBlockSize;
    private int i = 0;
    private int j = 0;
    private final int messageCount;

    public DataProcessThread(String threadName, String topic)
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        MessageSizeCalculator messageSizeCalculator = new MessageSizeCalculator();
        this.threadName = threadName;
        int indexOfDot = topic.indexOf(".");
        int length = topic.length();
        this.dbName = topic.substring(0, indexOfDot);
        this.tblName = topic.substring(indexOfDot + 1, length);
        this.hdfsWarehouse = config.getHDFSWarehouse();
        this.orcFileStripeSize = config.getOrcFileStripeSize();
        this.orcFileBufferSize = config.getOrcFileBufferSize();
        this.orcFileBlockSize = config.getOrcFileBlockSize();
        long blockSize = config.getBufferOfferBlockSize();
        long messageSize = messageSizeCalculator.caculate(topic);
        System.out.println("messageSize : " + messageSize);
        messageCount = (int) (blockSize / (messageSize + 1)); //+1 to
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
    }

    public String getName()
    {
        return threadName;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */

    /**
     * DataProcessThread run() is used to poll message from consumer buffer
     */
    @Override
    public void run()
    {
        int remainCount; //remaining message count
        if (messageCount > 0) { //blockSize is bigger then messageSize
            while (true) {
                if (isReadyToStop) { //loop end condition
                    System.out.println("Thread stop");
                    return;
                }
                remainCount = messageCount - messages.size();
//                System.out.println("remainCount : " + remainCount);
//                System.out.println("buffersize : " + buffer.size());
                for (; remainCount > 0 && buffer.size() > 0; remainCount = messageCount - messages.size()) {
                    buffer.drainTo(messages, remainCount);
                    System.out.println("remainCount : " + remainCount);
                    System.out.println("buffersize : " + buffer.size());
                }
                if (remainCount == 0) { //block is full
                    sort();
                    flush();
                    writeToMetaData();
                    clear();
                }
            }
        }
        else { //blockSize is small then messageSize
            System.out.println("Block size is too small to add one message!");
            System.out.println("Please increase the block size!");
        }
    }

    private void sort()
    {
        System.out.println("sort start!");
        for (Message message1 : messages) {
            if (messageLists.keySet().contains(message1.getKeyIndex())) {
                messageLists.get(message1.getKeyIndex()).add(message1);
            }
            else {
                messageLists.put(message1.getKeyIndex(), new LinkedList<>());
                messageLists.get(message1.getKeyIndex()).add(message1);
            }
        }
        //sort in every messageList
        for (Integer key : messageLists.keySet()) {
            Collections.sort(messageLists.get(key), new MessageListComparator());
        }
        for (Integer key : messageLists.keySet()) {
            System.out.println("messageLists.keySet() : key : " + key);
            for (Message message : messageLists.get(key)) {
                System.out.println("i = " + i++);
                System.out.println("DataProcessThread run() : message : " + message);
            }
        }
        System.out.println("sort end!");
    }

    private void flush()
    {
//        System.out.println("DefaultConsume : flush() : dbName : " + dbName);
//        System.out.println("DefaultConsume : flush() : tblName : " + tblName);
//        String file = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
//        System.out.println("file : " + file);
//        Path path = new Path(file);
//        Configuration conf = new Configuration();
//        FileSystem fs;
//        FSDataOutputStream output;
//        try {
//            fs = path.getFileSystem(conf);
//            output = fs.create(path);
//            for (Integer key : messageLists.keySet()) {
//                for (Message message : messageLists.get(key)) {
//                    String result = org.apache.commons.lang.StringUtils.join(message.getValues());
//                    result += message.getTimestamp();
//                    output.write(result.getBytes("UTF-8"));
//                }
//            }
//            output.flush();
//            output.close();
//            fs.close();
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.println("flush start!");
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
                    default:
                        System.out.println("default");
                        schema.addField(columnsNameList.getStr(i), TypeDescription.createString());
                }
            }
            String path = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
            System.out.println("path : " + path);
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
                for (Integer key : messageLists.keySet()) {
                    for (Message message : messageLists.get(key)) {
                        int rowCount = batch.size++;
                        String[] contents = message.getValues();
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
                    }
                    writer.addRowBatch(batch);
                    writer.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("flush end!");
    }

    private void writeToMetaData()
    {
        System.out.println("writeToMetaData start!");
        int fiberValue;
        long timeBegin;
        long timeEnd;
        String path;
        if (messages.get(0).getTimestamp().isPresent()) {
            timeBegin = messages.get(0).getTimestamp().get();
            timeEnd = messages.get(0).getTimestamp().get();
            for (Integer key : messageLists.keySet()) {
                for (int i = 0; i <= messageLists.get(key).size(); i++) {
                    if (messageLists.get(key).get(i).getTimestamp().isPresent()) {
                        if (messageLists.get(key).get(i).getTimestamp().get() < timeBegin) {
                            timeBegin = key;
                        }
                        if (messageLists.get(key).get(i).getTimestamp().get() > timeEnd) {
                            timeEnd = key;
                        }
                    }
                }
            }
            fiberValue = Integer.parseInt(messages.get(0).getValues()[messages.get(0).getKeyIndex()]);
            path = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
            metaClient.createBlockIndex(dbName, tblName, fiberValue, timeBegin, timeEnd, path);
        }
        //else ignore
        System.out.println("writeToMetaData end!");
    }

    public void clear()
    {
        messages.clear();
        messageLists.clear();
    }

    private void readyToStop()
    {
        this.isReadyToStop = true;
    }

    public void shutdown()
    {
        readyToStop();
    }
}
