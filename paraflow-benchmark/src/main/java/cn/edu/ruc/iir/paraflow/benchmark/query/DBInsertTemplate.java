package cn.edu.ruc.iir.paraflow.benchmark.query;

import io.airlift.tpch.RandomAlphaNumeric;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomPhoneNumber;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

public class DBInsertTemplate
        extends QueryTemplate
{
    private static final int BASE = 10_000_000;
    private int counter = 0;
    private RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(882244353, 25);
    private RandomPhoneNumber phoneNumber = new RandomPhoneNumber(1631138112);
    private RandomBoundedInt acctbalRandom = new RandomBoundedInt(291480230, -99999, 99999);
    private String[] segments = {"BUILDING", "AUTOMOBILE", "MACHINERY", "HOUSEHOLD", "FURNITURE"};
    private RandomText commentRandom = new RandomText(1334521707, TextPool.getDefaultTestPool(), 73);

    DBInsertTemplate(String table)
    {
        super(table);
    }

    @Override
    String makeQuery()
    {
        counter++;
        String q = "INSERT INTO " + table + " VALUES(" +
                (BASE + counter) +                                            // c_custkey
                ", 'Customer#0" + BASE + "'" +                                // c_name
                ", '" + addressRandom.nextValue() + "'" +                     // c_address
                ", " + (counter % 25) +                                       // c_nationkey
                ", '" + phoneNumber.nextValue(counter % 25) + "'" + // c_phone
                ", " + acctbalRandom.nextValue() +                            // c_acctbal
                ", '" + segments[counter % 5] + "'" +                         // c_mktsegment
                ", '" + commentRandom.nextValue() + "'" +                     // c_comment
                ");";
        addressRandom.rowFinished();
        phoneNumber.rowFinished();
        acctbalRandom.rowFinished();
        commentRandom.rowFinished();
        return q;
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.INSERT;
    }
}
