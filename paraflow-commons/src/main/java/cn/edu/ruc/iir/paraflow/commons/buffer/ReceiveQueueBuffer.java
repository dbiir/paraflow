package cn.edu.ruc.iir.paraflow.commons.buffer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReceiveQueueBuffer
{
    private final BlockingQueue<Message> queue;
    private static int blockSize;

    private ReceiveQueueBuffer(int offeredBlockSize)
    {
        this.blockSize = offeredBlockSize;
        queue = new LinkedBlockingQueue<>(offeredBlockSize);
    }

    private static class ReceiveQueueBufferHolder
    {
        private static final ReceiveQueueBuffer instance = new ReceiveQueueBuffer(blockSize);
    }

    public static final ReceiveQueueBuffer INSTANCE()
    {
        return ReceiveQueueBufferHolder.instance;
    }

    public Message poll()
    {
        return queue.poll();
    }

    public Message poll(long timeout) throws InterruptedException
    {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public Message take() throws InterruptedException
    {
        return queue.take();
    }

    public boolean offer(Message e)
    {
        return queue.offer(e);
    }

    public boolean offer(Message e, long timeout) throws InterruptedException
    {
        return queue.offer(e, timeout, TimeUnit.MILLISECONDS);
    }

    public void put(Message e) throws InterruptedException
    {
        queue.put(e);
    }
}
