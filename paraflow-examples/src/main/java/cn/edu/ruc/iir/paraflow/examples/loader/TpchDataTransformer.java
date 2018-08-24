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
                                  Integer.parseInt(valueParts[7]),        // shippriority
                                  valueParts[8],                          // ordercomment
                                  Integer.parseInt(valueParts[9]),        // linenumber
                                  Double.parseDouble(valueParts[10]),     // quantity
                                  Double.parseDouble(valueParts[11]),     // extendedprice
                                  Double.parseDouble(valueParts[12]),     // discount
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
