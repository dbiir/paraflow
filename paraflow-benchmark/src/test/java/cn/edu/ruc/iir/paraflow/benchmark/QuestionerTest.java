package cn.edu.ruc.iir.paraflow.benchmark;

import cn.edu.ruc.iir.paraflow.benchmark.query.DBQuestioner;
import cn.edu.ruc.iir.paraflow.benchmark.query.PrestoQuestioner;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class QuestionerTest
{
    @Test
    public void testDBQuestioner()
    {
        DBQuestioner questioner = new DBQuestioner("jdbc:postgresql://dbiir00:5432/tpch", "customer", "u");
        questioner.question();
    }

    @Test
    public void testPrestoQuestioner()
    {
        PrestoQuestioner questioner = new PrestoQuestioner("jdbc:presto://dbiir10:8080", "paraflow.test.tpch", "postgresql.public.customer");
        questioner.question();
    }
}
