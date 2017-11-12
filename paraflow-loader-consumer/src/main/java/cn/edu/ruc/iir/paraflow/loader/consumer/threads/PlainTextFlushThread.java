package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is a TEST class!!!
 * NOT USE IN PRODUCTION
 *
 * @author guodong
 */
public class PlainTextFlushThread extends DataFlushThread
{
    public PlainTextFlushThread(String threadName)
    {
        super(threadName);
    }

    @Override
    boolean flushData(BufferSegment segment)
    {
        try {
            System.out.println("Flush out!!!");
            String filePath = "/Users/Jelly/Desktop/testPlainFile";
            Random random = ThreadLocalRandom.current();
            long[] timestamps = segment.getTimestamps();
            long smallest = timestamps[0];
            long biggest = timestamps[timestamps.length - 1];
            filePath += smallest;
            filePath += biggest;
            filePath += random.nextLong();
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            while (segment.hasNext()) {
                String[] value = segment.getNext();
                for (String v : value) {
                    writer.write(v);
                }
                writer.newLine();
                writer.flush();
                System.out.println(Arrays.toString(value));
            }
            writer.close();
            segment.setFilePath(filePath);
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
