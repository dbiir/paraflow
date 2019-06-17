package cn.edu.ruc.iir.paraflow.benchmark.query;

import io.airlift.tpch.RandomAlphaNumeric;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomPhoneNumber;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

/**
 * paraflow
 *
 * @author guodong
 */
public class DBUpdateTemplate
        extends QueryTemplate
{
    private final RandomBoundedInt custkeyRandom = new RandomBoundedInt(292374330, 1, 1000000);
    private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155653, 25);
    private final RandomPhoneNumber phoneNumber = new RandomPhoneNumber(1732238332);
    private final RandomBoundedInt acctbalRandom = new RandomBoundedInt(273481450, -99999, 99999);
    private RandomText commentRandom = new RandomText(1249521607, TextPool.getDefaultTestPool(), 73);
    private int counter = 0;

    DBUpdateTemplate(String table)
    {
        super(table);
    }

    private String template1()
    {
        String q = "UPDATE " + table + " SET c_address='" + addressRandom.nextValue()
                + "' WHERE c_custkey=" + custkeyRandom.nextValue() + ";";
        addressRandom.rowFinished();
        custkeyRandom.rowFinished();
        return q;
    }

    private String template2()
    {
        String q = "UPDATE " + table + " SET c_phone='" + phoneNumber.nextValue(counter % 25)
                + "' WHERE c_custkey=" + custkeyRandom.nextValue() + ";";
        phoneNumber.rowFinished();
        custkeyRandom.rowFinished();
        return q;
    }

    private String template3()
    {
        String q = "UPDATE " + table + " SET c_acctbal=" + acctbalRandom.nextValue()
                + " WHERE c_custkey=" + custkeyRandom.nextValue() + ";";
        acctbalRandom.rowFinished();
        custkeyRandom.rowFinished();
        return q;
    }

    private String template4()
    {
        String q = "UPDATE " + table + " SET c_comment='" + commentRandom.nextValue()
                + "' WHERE c_custkey=" + custkeyRandom.nextValue() + ";";
        commentRandom.rowFinished();
        custkeyRandom.rowFinished();
        return q;
    }

    @Override
    String makeQuery()
    {
        counter++;
        if (counter % 4 == 0) {
            return template1();
        }
        if (counter % 4 == 1) {
            return template2();
        }
        if (counter % 4 == 2) {
            return template3();
        }
        if (counter % 4 == 3) {
            return template4();
        }
        return null;
    }

    @Override
    QueryGenerator.QueryType getType()
    {
        return QueryGenerator.QueryType.UPDATE;
    }
}
