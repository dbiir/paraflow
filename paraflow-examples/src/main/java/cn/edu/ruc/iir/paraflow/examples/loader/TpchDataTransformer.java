package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.loader.DataTransformer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * paraflow
 *
 * @author guodong
 */
public class TpchDataTransformer
        implements DataTransformer
{
    private static final Logger logger = LoggerFactory.getLogger(TpchDataTransformer.class);
    private final Kryo kryo;

    public TpchDataTransformer()
    {
        this.kryo = new Kryo();
        kryo.register(LineOrder.class, 10);
        kryo.register(byte[].class, 11);
        kryo.register(Object[].class, 12);
    }

    @Override
    public ParaflowRecord transform(byte[] value, int partition)
    {
        Input input = new ByteBufferInput(value);
        try {
            LineOrder lineOrder = kryo.readObject(input, LineOrder.class);
            lineOrder.setFiberId(partition);
            long custKey = lineOrder.getCustomerKey();
            lineOrder.setKey(custKey);
            lineOrder.setTimestamp(lineOrder.getCreation());
            input.close();
            return lineOrder;
        }
        catch (Exception e) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            logger.error(stringWriter.toString());
            e.printStackTrace();
            return null;
        }
    }
}
