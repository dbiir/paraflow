package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.collector.DefaultCollector;
import cn.edu.ruc.iir.paraflow.collector.ParaflowKafkaProducer;
import cn.edu.ruc.iir.paraflow.collector.utils.CollectorConfig;
import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.examples.collector.TpchDataSource;
import cn.edu.ruc.iir.paraflow.examples.loader.TpchDataTransformer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowKafkaConsumer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowSegment;
import cn.edu.ruc.iir.paraflow.loader.ParquetSegmentWriter;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CLERK;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.COMMIT_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CREATION;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CUSTOMER_KEY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.DISCOUNT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.EXTENDED_PRICE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINEITEM_COMMENT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINEORDER_KEY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINE_NUMBER;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_COMMENT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_PRIORITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_STATUS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.QUANTITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.RECEIPT_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.RETURN_FLAG;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_INSTRUCTIONS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_MODE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_PRIORITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.STATUS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.TAX;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.TOTAL_PRICE;

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
        kryo.register(byte[].class);
        kryo.register(Object[].class);

        final LoaderConfig config = LoaderConfig.INSTANCE();
        try {
            config.init();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        final MetaClient metaClient = new MetaClient("127.0.0.1", 10012);
        final int capacity = 8;
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(1000, 1, 1500, 0, 10000000);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        TpchDataTransformer transformer = new TpchDataTransformer();
        ParaflowRecord[][] content = new ParaflowRecord[1][];
        ParaflowRecord[] records = new ParaflowRecord[capacity];
        int counter = 0;
        long textSize = 0;
        Output output = new ByteBufferOutput(300, 2000);
        while (lineOrderIterator.hasNext() && counter < capacity) {
            LineOrder lineOrder = lineOrderIterator.next();
            kryo.writeObject(output, lineOrder);
            textSize += output.position();
            ParaflowRecord record = transformer.transform(output.toBytes(), 80);
            records[counter++] = record;
            output.reset();
        }
        content[0] = records;
        output.close();
        ParaflowSegment segment = new ParaflowSegment(content, new long[0], new long[0], 0.0d);
        segment.setPath("file:///Users/Jelly/Desktop/1");
        MetaProto.StringListType columnNames = metaClient.listColumns("test", "debug01");
        MetaProto.StringListType columnTypes = metaClient.listColumnsDataType("test", "debug01");
        final ParquetSegmentWriter segmentWriter = new ParquetSegmentWriter(segment, metaClient, null);
        long start = System.currentTimeMillis();
        if (segmentWriter.write(segment, columnNames, columnTypes)) {
            System.out.println("Binary size: " + (1.0 * textSize / 1024.0 / 1024.0) + " MB.");
        }
        long end = System.currentTimeMillis();
        System.out.println("Time cost: " + (end - start));
    }

    @Test
    public void testDebug()
    {
        try {
            DefaultCollector collector = new DefaultCollector();
            String dbName = "test";
            String tableName = "debug01";
            int partitionNum = 1;
            if (!collector.existsTable(dbName, tableName)) {
                String[] names = {LINEORDER_KEY.getColumnName(),
                        CUSTOMER_KEY.getColumnName(),
                        ORDER_STATUS.getColumnName(),
                        TOTAL_PRICE.getColumnName(),
                        ORDER_DATE.getColumnName(),
                        ORDER_PRIORITY.getColumnName(),
                        CLERK.getColumnName(),
                        SHIP_PRIORITY.getColumnName(),
                        ORDER_COMMENT.getColumnName(),
                        LINE_NUMBER.getColumnName(),
                        QUANTITY.getColumnName(),
                        EXTENDED_PRICE.getColumnName(),
                        DISCOUNT.getColumnName(),
                        TAX.getColumnName(),
                        RETURN_FLAG.getColumnName(),
                        STATUS.getColumnName(),
                        SHIP_DATE.getColumnName(),
                        COMMIT_DATE.getColumnName(),
                        RECEIPT_DATE.getColumnName(),
                        SHIP_INSTRUCTIONS.getColumnName(),
                        SHIP_MODE.getColumnName(),
                        LINEITEM_COMMENT.getColumnName(),
                        CREATION.getColumnName()
                };
                String[] types = {"bigint",
                        "bigint",
                        "varchar(1)",
                        "double",
                        "integer",
                        "varchar(15)",
                        "varchar(15)",
                        "integer",
                        "varchar(79)",
                        "integer",
                        "double",
                        "double",
                        "double",
                        "double",
                        "varchar(1)",
                        "varchar(1)",
                        "integer",
                        "integer",
                        "integer",
                        "varchar(25)",
                        "varchar(10)",
                        "varchar(44)",
                        "bigint"
                };
                collector.createTable(dbName, tableName, "parquet", 0, 22,
                        "cn.edu.ruc.iir.paraflow.examples.collector.BasicParaflowFiberPartitioner",
                        Arrays.asList(names), Arrays.asList(types));
            }
            if (!collector.existsTopic(dbName + "-" + tableName)) {
                collector.createTopic(dbName + "-" + tableName, partitionNum, (short) 1);
            }
            TpchDataSource dataSource = new TpchDataSource(1, 1, 1, 0, 1_000_000);
            Message message = dataSource.read();
            byte[] serialized = message.getValue();

            final LoaderConfig loaderConfig = LoaderConfig.INSTANCE();
            final CollectorConfig collectorConfig = CollectorConfig.INSTANCE();
            try {
                loaderConfig.init();
                collectorConfig.init();
            }
            catch (ConfigFileNotFoundException e) {
                e.printStackTrace();
            }
            ParaflowKafkaProducer producer = new ParaflowKafkaProducer(collectorConfig, 1000);
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(dbName + "-" + tableName, 0, message.getTimestamp(),
                            new byte[0], serialized);
            producer.sendMsg(record, serialized.length);
            producer.close();

            List<TopicPartition> topicPartitions = new ArrayList<>();
            TopicPartition topicPartition = new TopicPartition(dbName + "-" + tableName, 0);
            topicPartitions.add(topicPartition);
            ParaflowKafkaConsumer kafkaConsumer = new ParaflowKafkaConsumer(topicPartitions, loaderConfig.getProperties());
            Consumer<byte[], byte[]> consumer = kafkaConsumer.getConsumer();
            consumer.seekToBeginning(topicPartitions);
            TpchDataTransformer transformer = new TpchDataTransformer();
            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(1000);
                if (!consumerRecords.isEmpty()) {
                    ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.iterator().next();
                    byte[] content = consumerRecord.value();
                    ParaflowRecord record1 = transformer.transform(content, 0);
                    assert record.timestamp() == record1.getTimestamp();
                    break;
                }
                consumer.commitAsync();
            }
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
