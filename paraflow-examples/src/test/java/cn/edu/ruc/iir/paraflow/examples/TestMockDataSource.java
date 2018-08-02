package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.examples.collector.MockDataSource;
import org.testng.annotations.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestMockDataSource
{
    @Test
    public void testDataGenerationSpeed()
    {
        MockDataSource source = new MockDataSource();
        long counter = 0;
        long bytes = 0;
        long start = System.currentTimeMillis();
        while (true) {
            Message msg = source.read();
            counter++;
            bytes += msg.getValue().length;
            if (counter >= 100000) {
                long time = System.currentTimeMillis();
                System.out.printf("Cost: %d, speed: %.2fMB/sec\n", (time - start), 1000.0 * bytes / (time - start) / (1024.0 * 1024.0));
                System.out.println("Average length: " + (bytes * 1.0 / counter));
                System.out.flush();
                start = System.currentTimeMillis();
                counter = 0;
                bytes = 0;
            }
        }
    }
}
