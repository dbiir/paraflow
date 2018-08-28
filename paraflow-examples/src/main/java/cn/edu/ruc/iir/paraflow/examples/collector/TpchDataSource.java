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

    public TpchDataSource()
    {
        super("tpch");
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(10000, 1, 1500, 0, 1_000_000);
        this.lineOrderIterator = lineOrderIterable.iterator();
        this.kryo = new Kryo();
        kryo.register(LineOrder.class);
        kryo.register(byte[].class);
        kryo.register(Object[].class);
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
