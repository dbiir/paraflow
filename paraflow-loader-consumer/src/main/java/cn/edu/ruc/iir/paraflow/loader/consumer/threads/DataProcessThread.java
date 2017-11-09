package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageListComparator;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageSizeCalculator;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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
    private final String hdfsWarehouse;
    private final String dbName;
    private final String tblName;
    private String hdfsBasePath;
    private final int blockMessageNum;

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
        long blockSize = config.getBufferOfferBlockSize();
        long messageSize = messageSizeCalculator.caculate(topic);
        System.out.println("messageSize : " + messageSize);
        blockMessageNum = (int) (blockSize / (messageSize + 1)); //+1 to
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        this.hdfsBasePath = metaClient.getTable(dbName, tblName).getLocationUrl();
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
        if (blockMessageNum > 0) { //blockSize is bigger then messageSize
            while (true) {
                if (isReadyToStop) { //loop end condition
                    System.out.println("Thread stop");
                    return;
                }
                remainCount =  blockMessageNum - messages.size();
                while (remainCount > 0 && buffer.size() > 0) {
                    buffer.drainTo(messages, remainCount);
                    remainCount = blockMessageNum - messages.size();
                }
                if (remainCount == 0) { //block is full
                    sort();
                    writeToMetaData(flush());
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
        for (Message message : messages) {
            if (messageLists.keySet().contains(message.getFiberId().get())) {
                messageLists.get(message.getFiberId().get()).add(message);
            }
            else {
                messageLists.put(message.getFiberId().get(), new LinkedList<>());
                messageLists.get(message.getFiberId().get()).add(message);
            }
        }
        //sort in every messageList
        for (int key : messageLists.keySet()) {
            messageLists.get(key).sort(new MessageListComparator());
        }
    }

    /**
     * Flush out to disk.
     * @return file path
     * */
    private Path flush()
    {
        System.out.println("DefaultConsume : flush() : dbName : " + dbName);
        System.out.println("DefaultConsume : flush() : tblName : " + tblName);
        String file = String.format("%s/%s/%s", hdfsWarehouse, dbName, tblName);
        if (hdfsBasePath.endsWith("/")) {
            hdfsBasePath = hdfsBasePath.substring(0, hdfsBasePath.length() - 1);
        }
        System.out.println("file : " + file);
        Path path = new Path(file);
        Configuration conf = new Configuration();
        FileSystem fs;
        FSDataOutputStream output;
        try {
            fs = path.getFileSystem(conf);
            output = fs.create(path);
            for (int key : messageLists.keySet()) {
                for (Message message : messageLists.get(key)) {
                    String result = org.apache.commons.lang.StringUtils.join(message.getValues());
                    result += message.getTimestamp();
                    output.write(result.getBytes("UTF-8"));
                }
            }
            output.flush();
            output.close();
            fs.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return path;
    }

    private void writeToMetaData(Path path)
    {
//        long timeBegin;
//        long timeEnd;
//        if (messages.get(0).getTimestamp().isPresent()) {
//            for (int key : messageLists.keySet()) {
//                timeBegin = timeEnd = messageLists.get(key).get(0).getTimestamp().get();
//                for (int i = 0; i <= messageLists.get(key).size(); i++) {
//                    if (messageLists.get(key).get(i).getTimestamp().isPresent()) {
//                        if (messageLists.get(key).get(i).getTimestamp().get() < timeBegin) {
//                            timeBegin = key;
//                        }
//                        if (messageLists.get(key).get(i).getTimestamp().get() > timeEnd) {
//                            timeEnd = key;
//                        }
//                    }
//                }
//                metaClient.createBlockIndex(dbName, tblName, key, timeBegin, timeEnd, path.toString());
//            }
//        }
        //else ignore
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
