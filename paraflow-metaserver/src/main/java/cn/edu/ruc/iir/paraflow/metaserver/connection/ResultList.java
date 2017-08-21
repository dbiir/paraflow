package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.util.LinkedList;

/**
 * paraflow
 *
 */
public class ResultList
{
    private LinkedList<JDBCRecord> jdbcRecords;

    public ResultList()
    {
        jdbcRecords = new LinkedList<>();
    }

    public JDBCRecord get(int index)
    {
        return jdbcRecords.get(index);
    }

    public void add(JDBCRecord jdbcRecord)
    {
        jdbcRecords.add(jdbcRecord);
    }

    public int size()
    {
        return jdbcRecords.size();
    }

    public boolean isEmpty()
    {
        return jdbcRecords.isEmpty();
    }
}
