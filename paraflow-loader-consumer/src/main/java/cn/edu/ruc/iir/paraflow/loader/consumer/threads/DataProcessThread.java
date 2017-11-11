package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferPool;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.ConsumerConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class DataProcessThread extends DataThread
{
    private final MetaClient metaClient;
    private final ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();
    private List<TopicPartition> topicPartitions;
    private final BufferPool bufferPool;

    public DataProcessThread(String threadName, List<TopicPartition> topicPartitions)
    {
        ConsumerConfig config = ConsumerConfig.INSTANCE();
        this.threadName = threadName;
        this.topicPartitions = topicPartitions;
        long blockSize = config.getBufferOfferBlockSize();
        metaClient = new MetaClient(config.getMetaServerHost(),
                config.getMetaServerPort());
        this.bufferPool = new BufferPool(topicPartitions, blockSize, blockSize);
    }



    /**
     * DataProcessThread run() is used to poll message from consumer buffer
     */
    @Override
    public void run()
    {
        while (true) {
            if (isReadyToStop && buffer.isEmpty()) { //loop end condition
                System.out.println("Thread stop");
                return;
            }
            Message message = buffer.poll();
            if (message != null) {
                bufferPool.add(message);
            }
        }
    }

    /**
     * Flush out to disk.
     * @return file path
     * */
//    private Path flush()
//    {
//        Configuration conf = new Configuration();
//        FileSystem fs;
//        FSDataOutputStream output;
//        try {
//            fs = path.getFileSystem(conf);
//            output = fs.create(path);
//            output.flush();
//            output.close();
//            fs.close();
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }

//        return path;
//    }

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
}
