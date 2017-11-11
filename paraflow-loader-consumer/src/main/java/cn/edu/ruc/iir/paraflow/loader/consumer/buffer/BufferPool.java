package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import org.apache.kafka.common.TopicPartition;

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
    private long blockSize = 0L;

    private final long blockCapacity;
    private final List<Message>[] block;                         // THIS SHOULD BE A DYNAMIC ARRAY INSTEAD!! message buffer. each list records messages of a fiber
    private final long[] timestamps;                    // begin and end timestamps as metadata. each fiber contains two values (begin + end)
    private final Map<TopicPartition, Integer> fiberPartitionToBlockIndex; // mapping from fiber id to index of block array
    private final int timestampStride = 2;
    private final int beginTimeOffset = 0;
    private final int endTimeOffset   = 1;
    private final List<TopicPartition> fiberPartitions;

    private final FlushQueueBuffer flushQueueBuffer = FlushQueueBuffer.INSTANCE();

    public BufferPool(List<TopicPartition> fiberPartitions, long blockCapacity, long flushBufferCapacity)
    {
        int fiberNum = fiberPartitions.size();
        this.blockCapacity = blockCapacity;
        this.block = new LinkedList[fiberNum];
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
    }

    public void add(Message message)
    {
        if (blockSize + message.getValueSize() > blockCapacity) {
            while (!spillToFlushBuffer()) {
                // waiting
                System.out.println("Waiting for flush buffer");
            }
        }
        if (message.getFiberId().isPresent() && message.getTopic().isPresent()) {
            int fiberId = message.getFiberId().get();
            String fiberTopic = message.getTopic().get();
            TopicPartition fiber = new TopicPartition(fiberTopic, fiberId);
            block[fiberPartitionToBlockIndex.get(fiber)].add(message);
            blockSize += message.getValueSize();
        }
    }

    private boolean spillToFlushBuffer()
    {
        BufferSegment segment = flushQueueBuffer.addSegment(blockSize, timestamps, fiberPartitions);
        if (segment == null) {
            return false;
        }
        int index = 0;
        for (TopicPartition key : fiberPartitionToBlockIndex.keySet()) {
            List<Message> fiberMessages = block[fiberPartitionToBlockIndex.get(key)];
            fiberMessages.sort((o1, o2) -> {
                if (o1.getTimestamp().get().equals(o2.getTimestamp().get())) {
                    return 0;
                }
                return o1.getTimestamp().get() > o2.getTimestamp().get() ? 1 : -1;
            });
            timestamps[timestampStride * index + beginTimeOffset] =
                    fiberMessages.get(0).getTimestamp().get();
            timestamps[timestampStride * index + endTimeOffset] =
                    fiberMessages.get(fiberMessages.size() - 1).getTimestamp().get();
            fiberMessages.forEach(msg -> segment.addValueStride(msg.getValues()));
            fiberMessages.clear();
            index++;
        }
        blockSize = 0;
        return true;
    }
}
