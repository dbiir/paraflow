package cn.edu.ruc.iir.paraflow.loader.buffer;

import cn.edu.ruc.iir.paraflow.commons.Message;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReceiveQueueBuffer
{
    private final BlockingQueue<Message> queue;

    private ReceiveQueueBuffer()
    {
        queue = new LinkedBlockingQueue<>(100);
    }

    private static class ReceiveQueueBufferHolder
    {
        private static final ReceiveQueueBuffer instance = new ReceiveQueueBuffer();
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

    public int drainTo(LinkedList<Message> messages)
    {
        return queue.drainTo(messages);
    }

    public int drainTo(LinkedList<Message> messages, int maxElements)
    {
        return queue.drainTo(messages, maxElements);
    }

    public int remainingCapacity()
    {
        return queue.remainingCapacity();
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    public void put(Message e) throws InterruptedException
    {
        queue.put(e);
    }

    public void add(Message e)
    {
        queue.add(e);
    }

    public int size()
    {
        return queue.size();
    }
}
