package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.commons.TopicFiber;
import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageSizeCalculator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * This is a pipeline for flushing out messages
 *
 * @author guodong
 */
public class BufferPool
{
    private static final int TIMESTAMP_STRIDE = 2;
    private static final int BEGIN_TIME_OFFSET = 0;
    private static final int END_TIME_OFFSET = 1;

    private long blockSize = 0L;

    private final long blockCapacity;
    private final List<Message>[] block;                // THIS SHOULD BE A DYNAMIC ARRAY INSTEAD!! message buffer. each list records messages of a fiber
    private final long[] timestamps;                    // begin and end timestamps as metadata. each fiber contains two values (begin + end)
    private final Map<TopicFiber, Integer> fiberPartitionToBlockIndex; // mapping from fiber id to index of block array
    private final List<TopicFiber> fiberPartitions;

    private final FlushQueueBuffer flushQueueBuffer = FlushQueueBuffer.INSTANCE();

    public BufferPool(List<TopicFiber> fiberPartitions, long blockCapacity, long flushBufferCapacity)
    {
        int fiberNum = fiberPartitions.size();
        this.blockCapacity = blockCapacity;
        this.block = new List[fiberNum];
        this.timestamps = new long[fiberNum * 2];
        this.fiberPartitionToBlockIndex = new TreeMap<>(
                (o1, o2) -> {
                    if (Objects.equals(o1.toString(), o2.toString())) {
                        return 0;
                    }
                    return o1.toString().compareTo(o2.toString());
                }
        );
        this.flushQueueBuffer.setBufferCapacity(flushBufferCapacity);
        this.fiberPartitions = fiberPartitions;

        for (int i = 0; i < fiberPartitions.size(); i++) {
            fiberPartitionToBlockIndex.put(fiberPartitions.get(i), i);
        }
        for (int i = 0; i < fiberNum; i++) {
            block[i] = new LinkedList<>();
        }
    }

    public void add(Message message)
    {
        String topic = message.getTopic().get();
        long messageSize = MessageSizeCalculator.caculate(topic);
        System.out.println("Block size: " + blockSize + ", current message size: " + messageSize + "capacity: " + blockCapacity);
        if (blockSize + messageSize > blockCapacity) {
            while (!spillToFlushBuffer()) {
                // waiting
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        else if (message.getFiberId().isPresent() && message.getTopic().isPresent()) {
            int fiberId = message.getFiberId().get();
            String fiberTopic = message.getTopic().get();
            TopicFiber fiber = new TopicFiber(fiberTopic, fiberId);
            block[fiberPartitionToBlockIndex.get(fiber)].add(message);
            blockSize += messageSize; // all message's value size put together.
        }
        else {
            System.out.println("Message fiberId or topic not present");
        }
    }

    private boolean spillToFlushBuffer()
    {
        BufferSegment segment = new BufferSegment(blockSize, timestamps, fiberPartitions);
        int index = 0;
        for (TopicFiber key : fiberPartitionToBlockIndex.keySet()) {
            List<Message> fiberMessages = block[fiberPartitionToBlockIndex.get(key)];
            if (fiberMessages.size() == 0) {
                continue;
            }
            fiberMessages.sort((o1, o2) -> {
                if (o1.getTimestamp().get().equals(o2.getTimestamp().get())) {
                    return 0;
                }
                return o1.getTimestamp().get() > o2.getTimestamp().get() ? 1 : -1;
            });
            timestamps[TIMESTAMP_STRIDE * index + BEGIN_TIME_OFFSET] =
                    fiberMessages.get(0).getTimestamp().get();
            timestamps[TIMESTAMP_STRIDE * index + END_TIME_OFFSET] =
                    fiberMessages.get(fiberMessages.size() - 1).getTimestamp().get();
            fiberMessages.forEach(msg -> segment.addValue(msg.getValue()));
            fiberMessages.clear();
            index++;
        }
        while (!flushQueueBuffer.addSegment(blockSize, segment)) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        blockSize = 0;
        System.out.println("Spilled to flush queue buffer");
        return true;
    }
}
