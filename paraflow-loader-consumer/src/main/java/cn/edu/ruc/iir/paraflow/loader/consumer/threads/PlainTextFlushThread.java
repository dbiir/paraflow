package cn.edu.ruc.iir.paraflow.loader.consumer.threads;

import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.BufferSegment;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * paraflow
 *
 * @author guodong
 */
public class PlainTextFlushThread extends DataFlushThread
{
    @Override
    boolean flushData(BufferSegment segment)
    {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/Jelly/Desktop/testPlainFile"));
            while (segment.hasNext()) {
                String[] value = segment.getNext();
                for (String v : value) {
                    writer.write(v);
                }
                writer.newLine();
            }
            writer.close();
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
