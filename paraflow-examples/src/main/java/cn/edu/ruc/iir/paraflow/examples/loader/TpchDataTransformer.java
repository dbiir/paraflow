package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.loader.DataTransformer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowRecord;

/**
 * paraflow
 *
 * @author guodong
 */
public class TpchDataTransformer
        implements DataTransformer
{
    @Override
    public ParaflowRecord transform(byte[] value, int partition)
    {
        String valueStr = new String(value);
        String[] valueParts = valueStr.split("\\|");
        int key = Integer.parseInt(valueParts[0]);
        long timestamp = Long.parseLong(valueParts[22]);

        return new ParaflowRecord(key, timestamp, partition,
                                  key,                                    // custkey
                                  Long.parseLong(valueParts[1]),          // lineorderkey
                                  valueParts[2],                          // orderstatus
                                  Double.parseDouble(valueParts[3]),      // totalprice
                                  Integer.parseInt(valueParts[4]),        // orderdate
                                  valueParts[5],                          // orderpriority
                                  valueParts[6],                          // clerk
                                  Integer.parseInt(valueParts[6]),        // shippriority
                                  valueParts[7],                          // ordercomment
                                  Integer.parseInt(valueParts[8]),        // linenumber
                                  Double.parseDouble(valueParts[9]),      // quantity
                                  Double.parseDouble(valueParts[10]),     // extendedprice
                                  Double.parseDouble(valueParts[11]),     // discount
                                  Double.parseDouble(valueParts[12]),     // parts
                                  Double.parseDouble(valueParts[13]),     // tax
                                  valueParts[14],                         // returnflag
                                  valueParts[15],                         // linestatus
                                  Integer.parseInt(valueParts[16]),       // shipdate
                                  Integer.parseInt(valueParts[17]),       // commitdate
                                  Integer.parseInt(valueParts[18]),       // receiptdate
                                  valueParts[19],                         // shipinstruct
                                  valueParts[20],                         // shipmode
                                  valueParts[21],                         // lineitemcomment
                                  timestamp                               // creation
        );
    }
}
