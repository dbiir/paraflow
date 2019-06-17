package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.collector.DefaultCollector;
import cn.edu.ruc.iir.paraflow.collector.StringMessageSerializationSchema;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.util.Arrays;

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
public class BasicCollector
{
    private BasicCollector()
    {
    }

    public static void main(String[] args)
    {
        if (args.length != 9) {
            System.out.println("Usage: dbName tableName parallelism partitionNum sf part partCount minCustKey maxCustKey, part starts from 1");
            System.exit(-1);
        }
        String dbName = args[0];
        String tableName = args[1];
        int parallelism = Integer.parseInt(args[2]);
        int partitionNum = Integer.parseInt(args[3]);
        int sf = Integer.parseInt(args[4]);
        int part = Integer.parseInt(args[5]);
        int partCount = Integer.parseInt(args[6]);
        long minCustKey = Long.parseLong(args[7]);
        long maxCustKey = Long.parseLong(args[8]);
        try {
            DefaultCollector<String> collector = new DefaultCollector<>();
            if (!collector.existsDatabase(dbName)) {
                collector.createDatabase(dbName);
            }
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

            for (int i = 0; i < parallelism; i++) {
                DataSource dataSource = new TpchDataSource(sf, part, partCount, minCustKey, maxCustKey);
                collector.collect(dataSource, 1, 22,
                        new BasicParaflowFiberPartitioner(partitionNum),
                        new StringMessageSerializationSchema<>(),
                        new MockDataSink(dbName, tableName));
            }
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
