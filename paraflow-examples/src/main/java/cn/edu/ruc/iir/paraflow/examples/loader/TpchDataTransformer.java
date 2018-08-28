package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.loader.DataTransformer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;

/**
 * paraflow
 *
 * @author guodong
 */
public class TpchDataTransformer
        implements DataTransformer
{
    private final Kryo kryo;

    public TpchDataTransformer()
    {
        this.kryo = new Kryo();
        kryo.register(LineOrder.class);
        kryo.register(byte[].class);
        kryo.register(Object[].class);
    }

    @Override
    public ParaflowRecord transform(byte[] value, int partition)
    {
        Input input = new ByteBufferInput(value);
        LineOrder lineOrder = kryo.readObject(input, LineOrder.class);
        input.close();
        lineOrder.setFiberId(partition);
        lineOrder.setKey(lineOrder.getCustomerKey());
        lineOrder.setTimestamp(lineOrder.getCreation());
        return lineOrder;
    }
}
