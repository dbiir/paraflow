package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.utils.BytesUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;

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
    private final Kryo kryo;
    private final Output output;

    public TpchDataSource(int sf, int part, int partCount, long minCustomerKey, long maxCustomerKey)
    {
        super("tpch");
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(sf, part, partCount, minCustomerKey, maxCustomerKey);
        this.lineOrderIterator = lineOrderIterable.iterator();
        this.kryo = new Kryo();
        kryo.register(LineOrder.class, 10);
        kryo.register(byte[].class, 11);
        kryo.register(Object[].class, 12);
        this.output = new ByteBufferOutput(300, 2000);
    }

    @Override
    public Message read()
    {
        if (lineOrderIterator.hasNext()) {
            LineOrder lineOrder = lineOrderIterator.next();
            kryo.writeObject(output, lineOrder);
            Message message = new Message(BytesUtils.toBytes((int) lineOrder.getCustomerKey()),
                    output.toBytes(),
                    lineOrder.getCreation());
            output.reset();
            return message;
        }
        return null;
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

    @Override
    public String toString()
    {
        return "TpchDataSource";
    }
}
