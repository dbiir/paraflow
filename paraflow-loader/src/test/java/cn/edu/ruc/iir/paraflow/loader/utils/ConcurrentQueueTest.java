package cn.edu.ruc.iir.paraflow.loader.utils;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class ConcurrentQueueTest
{
    @Test
    public void testPushPullQueuePerformance()
    {
        BlockingQueue<ParaflowRecord> blockingQueue = new PushPullBlockingQueue<>(500);
        Sender sender = new Sender(blockingQueue);
        Receiver receiver = new Receiver(blockingQueue);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(sender);
        executorService.submit(receiver);
        try {
            executorService.awaitTermination(100, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDisruptorQueuePerformance()
    {
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        BlockingQueue<ParaflowRecord> blockingQueue
                = new DisruptorBlockingQueue<>(500, SpinPolicy.SPINNING);
        for (int i = 0; i < 4; i++) {
            Sender sender = new Sender(blockingQueue);
            executorService.submit(sender);
        }
        Receiver receiver = new Receiver(blockingQueue);
        executorService.submit(receiver);
        try {
            executorService.awaitTermination(100, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class MockParaflowRecord
            extends ParaflowRecord
    {
        @Override
        public Object getValue(int idx)
        {
            return null;
        }
    }

    private class Sender
            implements Runnable
    {
        private final BlockingQueue<ParaflowRecord> concurrentQueue;

        Sender(BlockingQueue<ParaflowRecord> concurrentQueue)
        {
            this.concurrentQueue = concurrentQueue;
        }

        private void sendMsg()
        {
            try {
                MockParaflowRecord record = new MockParaflowRecord();
                record.setFiberId(0);
                record.setKey(1);
                record.setTimestamp(System.currentTimeMillis());
                concurrentQueue.put(record);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run()
        {
            while (true) {
                sendMsg();
            }
        }
    }

    private class Receiver
            implements Runnable
    {
        private final BlockingQueue<ParaflowRecord> concurrentQueue;

        Receiver(BlockingQueue<ParaflowRecord> concurrentQueue)
        {
            this.concurrentQueue = concurrentQueue;
        }

        @Override
        public void run()
        {
            while (true) {
                ParaflowRecord record;
                try {
                    record = concurrentQueue.take();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
