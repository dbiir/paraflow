package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.util.ArrayList;

/**
 * paraflow
 *
 * @author guodong
 */
public class ResultList
{
    private static ArrayList<JDBCRecord> jdbcRecords;

    public void JDBCRecord()
    {
        jdbcRecords = new ArrayList<>();
    }

    public JDBCRecord get(int index)
    {
        return jdbcRecords.get(index);
    }

    public void add(JDBCRecord jdbcRecord)
    {
        jdbcRecords.add(jdbcRecord);
    }
}
