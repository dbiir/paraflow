package cn.edu.ruc.iir.paraflow.metaserver.connection;

import java.util.LinkedList;

/**
 * paraflow
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
        if (index < jdbcRecords.size()) {
            return jdbcRecords.get(index);
        }
        else {
            return null;
        }
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
