package cn.edu.ruc.iir.paraflow.loader.consumer.buffer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * This is a pipeline for flushing out messages
 *
 * @author guodong
 */
public class BufferBlock
{
    private long blockMinTimestamp = Long.MIN_VALUE;
    private long blockMaxTimestamp = Long.MIN_VALUE;
    private long blockSize = 0L;

    private final String dbName;
    private final String tblName;
    private final long blockCapacity;
    private final List[] block;                         // message buffer. each list records messages of a fiber
    private final long[] timestamps;                    // begin and end timestamps as metadata. each fiber contains two values (begin + end)
    private final Map<Integer, Integer> fiberIdToIndex; // mapping from fiber id to index of block array
    private final int timestampStride = 2;
    private final int beginTimeOffset = 0;
    private final int endTimeOffset   = 1;

    private final FlushBuffer flushBuffer = FlushBuffer.INSTANCE();

    public BufferBlock(String dbName, String tblName, long blockCapacity, int[] fiberIds, long flushBufferCapacity)
    {
        int fiberNum = fiberIds.length;
        this.dbName = dbName;
        this.tblName = tblName;
        this.blockCapacity = blockCapacity;
        this.block = new List[fiberNum];
        this.timestamps = new long[fiberNum * 2];
        this.fiberIdToIndex = new TreeMap<>(
                (o1, o2) -> {
                    if (Objects.equals(o1, o2)) {
                        return 0;
                    }
                    return o1 > o2 ? 1 : -1;
                }
        );
        this.flushBuffer.setBufferCapacity(flushBufferCapacity);

        for (int i = 0; i < fiberIds.length; i++) {
            fiberIdToIndex.put(fiberIds[i], i);
        }
    }

    public void add(Message message)
    {
        if (blockSize + message.getValueSize() > blockCapacity) {
            spillToFlushBuffer();
        }
        int fiberId = message.getFiberId().get();
        block[fiberIdToIndex.get(fiberId)].add(message);
        blockSize += message.getValueSize();
    }

    private void spillToFlushBuffer()
    {
        int[] fiberIds = new int[fiberIdToIndex.size()];
        Object[] objects = fiberIdToIndex.keySet().toArray();
        for (int i = 0; i < fiberIdToIndex.size(); i++) {
            fiberIds[i] = (int) objects[i];
        }
        BufferSegment segment = flushBuffer.newSegment(blockSize, timestamps, fiberIds);
        for (int i = 0; i < fiberIds.length; i++) {
            List<Message> fiberMessages = block[fiberIdToIndex.get(fiberIds[i])];
            fiberMessages.sort((o1, o2) -> {
                if (o1.getTimestamp().get().equals(o2.getTimestamp().get())) {
                    return 0;
                }
                return o1.getTimestamp().get() > o2.getTimestamp().get() ? 1 : -1;
            });
            timestamps[timestampStride * i + beginTimeOffset] =
                    fiberMessages.get(0).getTimestamp().get();
            timestamps[timestampStride * i + endTimeOffset] =
                    fiberMessages.get(fiberMessages.size() - 1).getTimestamp().get();
            fiberMessages.forEach(msg -> segment.addValueStride(msg.getValues()));
            fiberMessages.clear();
        }
    }
}
