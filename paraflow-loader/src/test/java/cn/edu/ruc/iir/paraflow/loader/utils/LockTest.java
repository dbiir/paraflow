package cn.edu.ruc.iir.paraflow.loader.utils;

import org.junit.Test;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public class LockTest
{
    @Test
    public void readWriteLockTest()
    {
        Case aCase = new Case();
        Writer writer = new Writer(aCase);
        new Thread(writer).start();
        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (aCase.level == Level.SECOND) {
            Reader reader = new Reader(aCase);
            new Thread(reader).start();
        }
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    enum Level
    {
        FIRST, SECOND, THIRD
    }

    private class Case
    {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile Level level;

        Case()
        {
            level = Level.FIRST;
        }

        void readLock()
        {
            lock.readLock().lock();
        }

        void writeLock()
        {
            lock.writeLock().lock();
        }

        void readUnLock()
        {
            lock.readLock().unlock();
        }

        void writeUnLock()
        {
            lock.writeLock().unlock();
        }

        void updateLevel(Level level)
        {
            this.level = level;
        }
    }

    private class Writer
            implements Runnable
    {
        private Case aCase;

        Writer(Case aCase)
        {
            this.aCase = aCase;
        }

        @Override
        public void run()
        {
            if (aCase.level == Level.FIRST) {
                System.out.println("Writing as second.");
                aCase.updateLevel(Level.SECOND);
                return;
            }
            System.out.println("Write nothing.");
        }
    }

    private class Reader
            implements Runnable
    {
        private Case aCase;

        Reader(Case aCase)
        {
            this.aCase = aCase;
        }

        @Override
        public void run()
        {
            if (aCase.level == Level.SECOND) {
                System.out.println("Read as third");
                aCase.updateLevel(Level.THIRD);
                return;
            }
            System.out.println("Read nothing.");
        }
    }
}
