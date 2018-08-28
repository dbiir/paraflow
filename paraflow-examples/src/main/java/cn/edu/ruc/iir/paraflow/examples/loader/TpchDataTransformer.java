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
    private static final Kryo kryo = new Kryo();

    public TpchDataTransformer()
    {
        kryo.register(LineOrder.class);
        kryo.register(Object[].class);
        kryo.register(byte[].class);
    }

    @Override
    public ParaflowRecord transform(byte[] value, int partition)
    {
        Input input = new ByteBufferInput(value);
        LineOrder lineOrder = kryo.readObject(input, LineOrder.class);
        input.close();
        lineOrder.setFiberId(partition);
        return lineOrder;
    }
}
