package cn.edu.ruc.iir.paraflow.loader.producer.buffer;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class BlockingQueueBuffer
{
    private final BlockingQueue<Message> queue;

    private BlockingQueueBuffer()
    {
        queue = new LinkedBlockingQueue<>();
    }

    private static class BlockingQueueBufferHolder<K>
    {
        private static final BlockingQueueBuffer instance = new BlockingQueueBuffer();
    }

    public static final BlockingQueueBuffer INSTANCE()
    {
        return BlockingQueueBufferHolder.instance;
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
