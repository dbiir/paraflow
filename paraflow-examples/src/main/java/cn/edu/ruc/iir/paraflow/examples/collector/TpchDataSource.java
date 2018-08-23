package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.BytesUtils;

import java.util.Iterator;
import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class TpchDataSource
        extends DataSource
{
    private final Iterator<LineOrder> lineOrderIterator;

    public TpchDataSource()
    {
        super("tpch");
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(10000, 1, 1500);
        lineOrderIterator = lineOrderIterable.iterator();
    }

    @Override
    public Message read()
    {
        if (lineOrderIterator.hasNext()) {
            LineOrder lineOrder = lineOrderIterator.next();
            return new Message(BytesUtils.toBytes((int) lineOrder.getCustomerKey()),
                               lineOrder.toLine().getBytes(),
                               lineOrder.getCreation());
        }
        return null;
    }

    @Override
    public String toString()
    {
        return "TpchDataSource";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode("tpch-source");
    }

    @Override
    public boolean equals(Object other)
    {
        return false;
    }
}
