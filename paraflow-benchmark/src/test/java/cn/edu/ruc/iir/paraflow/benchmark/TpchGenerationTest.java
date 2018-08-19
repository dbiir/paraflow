package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.model.Customer;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.benchmark.model.Nation;
import cn.edu.ruc.iir.paraflow.benchmark.model.Region;
import org.junit.Test;

import java.util.Iterator;

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
        Iterable<Customer> customerGenerator = TpchTable.CUSTOMER.createGenerator(1, 1, 1500);
        Iterator<Customer> customerIterator = customerGenerator.iterator();
        long start = System.currentTimeMillis();
        long counter = 0;
        long msgLen = 0;
        while (customerIterator.hasNext()) {
            Customer customer = customerIterator.next();
            msgLen += customer.toLine().length();
            long customerId = customer.getCustomerKey();
            System.out.println(customerId);
            counter++;
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("Generate " + counter + " messages in " + duration + "ms (" + (1.0 * msgLen / duration) + " KB/s)");
    }

    @Test
    public void testLineOrderGeneration()
    {
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(1, 1, 1500);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        long start = System.currentTimeMillis();
        long counter = 0;
        long msgLen = 0;
        while (lineOrderIterator.hasNext()) {
            LineOrder lineOrder = lineOrderIterator.next();
            msgLen += lineOrder.toLine().length();
            System.out.println(lineOrder.getCustomerKey());
            counter++;
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("Generate " + counter + " messages in " + duration + "ms (" + (1.0 * msgLen / duration) + " KB/s)");
    }

    @Test
    public void testNationGeneration()
    {
        Iterable<Nation> nationIterable = TpchTable.NATION.createGenerator(1, 1, 1500);
        Iterator<Nation> nationIterator = nationIterable.iterator();
        while (nationIterator.hasNext()) {
            System.out.println(nationIterator.next().toLine());
        }
    }

    @Test
    public void testRegionGeneration()
    {
        Iterable<Region> regionIterable = TpchTable.REGION.createGenerator(1, 1, 1500);
        Iterator<Region> regionIterator = regionIterable.iterator();
        while (regionIterator.hasNext()) {
            System.out.println(regionIterator.next().toLine());
        }
    }
}
