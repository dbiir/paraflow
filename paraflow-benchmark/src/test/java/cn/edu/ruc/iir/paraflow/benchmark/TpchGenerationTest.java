package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.model.Customer;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.benchmark.model.Nation;
import cn.edu.ruc.iir.paraflow.benchmark.model.Region;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * paraflow
 *
 * @author guodong
 */
public class TpchGenerationTest
{
    @Test
    public void testCustomerGeneration()
    {
        Iterable<Customer> customerGenerator = TpchTable.CUSTOMER.createGenerator(10000, 1, 1500, 0, 0);
        Iterator<Customer> customerIterator = customerGenerator.iterator();
        long start = System.currentTimeMillis();
        long counter = 0;
        long msgLen = 0;
        while (customerIterator.hasNext()) {
            Customer customer = customerIterator.next();
            msgLen += customer.toLine().length();
            counter++;
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("Generate " + counter + " messages in " + duration +
                "ms (" + (1.0 * msgLen / duration) + " KB/s)");
    }

    @Test
    public void testGenerateCustomerFile()
    {
        Iterable<Customer> customerGenerator = TpchTable.CUSTOMER.createGenerator(10000, 1, 1500, 0, 0);
        Iterator<Customer> customerIterator = customerGenerator.iterator();
        File file = new File("/Users/Jelly/Developer/paraflow/paraflow-benchmark/data/customer.tbl");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            while (customerIterator.hasNext()) {
                writer.write(customerIterator.next().toLine());
                writer.newLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLineOrder()
    {
        int part = 1;
        Iterable<LineOrder> lineOrderIterable =
                TpchTable.LINEORDER.createGenerator(4, part, 1, 0, 10000000);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        int counter = 1;
        LineOrder lineOrder = lineOrderIterator.next();
        System.out.println(lineOrder.getLineOrderKey());
        long maxLineOrderKey = 0;
        while (lineOrderIterator.hasNext()) {
            counter++;
            LineOrder lineOrder1 = lineOrderIterator.next();
            maxLineOrderKey = lineOrder1.getLineOrderKey();
        }
        System.out.println(maxLineOrderKey);
        System.out.println(counter);
    }

    @Test
    public void testLineOrderGeneration()
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 1; i <= 4; i++) {
            LineOrderGenerationThread generationThread = new LineOrderGenerationThread(i);
            executorService.submit(generationThread);
        }
        try {
            executorService.awaitTermination(3600, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGenerateNationFile()
    {
        Iterable<Nation> nationIterable = TpchTable.NATION.createGenerator(1, 1, 1500, 0, 0);
        Iterator<Nation> nationIterator = nationIterable.iterator();
        File file = new File("/Users/Jelly/Developer/paraflow/paraflow-benchmark/data/nation.tbl");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            while (nationIterator.hasNext()) {
                writer.write(nationIterator.next().toLine());
                writer.newLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGenerateRegionFile()
    {
        Iterable<Region> regionIterable = TpchTable.REGION.createGenerator(1, 1, 1500, 0, 0);
        Iterator<Region> regionIterator = regionIterable.iterator();
        File file = new File("/Users/Jelly/Developer/paraflow/paraflow-benchmark/data/region.tbl");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            while (regionIterator.hasNext()) {
                writer.write(regionIterator.next().toLine());
                writer.newLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class LineOrderGenerationThread
            implements Runnable
    {
        private final int part;

        public LineOrderGenerationThread(int part)
        {
            this.part = part;
        }

        @Override
        public void run()
        {
            Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(1, part, 8, 0, 10000000);
            Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
            long start = System.currentTimeMillis();
            long counter = 0;
            long msgLen = 0;
            long currentTimestamp = 0;
            long creationCounter = 0;
            while (lineOrderIterator.hasNext()) {
                LineOrder lineOrder = lineOrderIterator.next();
//                msgLen += lineOrder.toLine().length();
                long creation = lineOrder.getCreation();
                if (creation != currentTimestamp) {
                    System.out.println(creation);
                    creationCounter++;
                    currentTimestamp = creation;
                }
                counter++;
            }
            System.out.println("Creation counter: " + creationCounter);
            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.println("Generate " + counter + " messages in " + duration +
                    "ms (" + (1.0 * msgLen / duration) + " KB/s)");
        }
    }
}
