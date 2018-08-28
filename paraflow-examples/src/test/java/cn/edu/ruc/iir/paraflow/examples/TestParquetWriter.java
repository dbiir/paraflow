package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.examples.loader.TpchDataTransformer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowSegment;
import cn.edu.ruc.iir.paraflow.loader.ParquetSegmentWriter;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * paraflow
 *
 * @author guodong
 */
public class TestParquetWriter
{
    @Test
    public void testParquetCompression()
    {
        final Kryo kryo = new Kryo();
        kryo.register(LineOrder.class);
        kryo.register(Object[].class);
        kryo.register(byte[].class);

        final LoaderConfig config = LoaderConfig.INSTANCE();
        try {
            config.init();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        final MetaClient metaClient = new MetaClient("127.0.0.1", 10012);
        final int capacity = 800_000;
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(1000, 1, 1500, 0, 10000000);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        TpchDataTransformer transformer = new TpchDataTransformer();
        ParaflowRecord[] records = new ParaflowRecord[capacity];
        int counter = 0;
        long textSize = 0;
        Output output = new ByteBufferOutput(200, 2000);
        while (lineOrderIterator.hasNext() && counter < capacity) {
            LineOrder lineOrder = lineOrderIterator.next();
            kryo.writeObject(output, lineOrder);
            textSize += output.position();
            ParaflowRecord record = transformer.transform(output.toBytes(), 80);
            records[counter++] = record;
            output.reset();
        }
        output.close();
        ParaflowSegment segment = new ParaflowSegment(records, new long[0], new long[0]);
        segment.setPath("file:///Users/Jelly/Desktop/1");
        long metaStart = System.currentTimeMillis();
        MetaProto.StringListType columnNames = metaClient.listColumns("test", "tbl082702");
        MetaProto.StringListType columnTypes = metaClient.listColumnsDataType("test", "tbl082702");
        long metaEnd = System.currentTimeMillis();
        System.out.println("Meta cost: " + (metaEnd - metaStart));
        final ParquetSegmentWriter segmentWriter = new ParquetSegmentWriter(segment, 0, 79, metaClient, null);
        System.out.println("Begin writing...");
        long start = System.currentTimeMillis();
        if (segmentWriter.write(segment, columnNames, columnTypes)) {
            System.out.println("Binary size: " + (1.0 * textSize / 1024.0 / 1024.0) + " MB.");
        }
        long end = System.currentTimeMillis();
        System.out.println("Time cost: " + (end - start));
    }
}
