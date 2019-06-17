package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow benchmark query client
 * args: server type(ap|tp|htap) table joinTable mode
 * <p>
 * java -jar paraflow-benchmark-1.0-alpha1-allinone.jar jdbc:postgresql://dbiir00:5432/tpch TP customer u
 * java -jar paraflow-benchmark-1.0-alpha1-allinone.jar jdbc:postgresql://dbiir00:5432/tpch TP customer ui
 * java -jar paraflow-benchmark-1.0-alpha1-allinone.jar jdbc:postgresql://dbiir00:5432/tpch TP customer uid
 * <p>
 * java -jar paraflow-benchmark-1.0-alpha1-allinone.jar jdbc:presto://dbiir10:8080 AP paraflow.test.tpch postgresql.public.customer
 *
 * @author guodong
 */
public class QueryClient
{
    private QueryClient()
    {
    }

    public static void main(String[] args)
    {
        String serverUrl = args[0];
        String type = args[1];
        String table = args[2];
        String joinTable = "";
        String mode = "";
        if (args.length == 4) {
            joinTable = args[3];
            mode = args[3].trim();
        }
        switch (type.toUpperCase()) {
            case "AP":
                PrestoQueryRunner apQueryRunner = new PrestoQueryRunner(serverUrl, table, joinTable);
                new Thread(apQueryRunner).start();
                break;
            case "TP":
                DBQueryRunner tpQueryRunner = new DBQueryRunner(serverUrl, table, mode);
                new Thread(tpQueryRunner).start();
                break;
            case "HTAP":
                PrestoQueryRunner prestoQueryRunner = new PrestoQueryRunner(serverUrl, table, joinTable);
                DBQueryRunner dbQueryRunner = new DBQueryRunner(serverUrl, table, mode);
                new Thread(prestoQueryRunner).start();
                new Thread(dbQueryRunner).start();
                break;
            default:
                System.out.println("No matching mode.");
        }
    }

    private static class PrestoQueryRunner
            implements Runnable
    {
        private final PrestoQuestioner questioner;

        PrestoQueryRunner(String serverUrl, String table, String joinTable)
        {
            this.questioner = new PrestoQuestioner(serverUrl, table, joinTable);
        }

        @Override
        public void run()
        {
            questioner.question();
        }
    }

    private static class DBQueryRunner
            implements Runnable
    {
        private final DBQuestioner questioner;

        DBQueryRunner(String serverUrl, String table, String mode)
        {
            this.questioner = new DBQuestioner(serverUrl, table, mode);
        }

        @Override
        public void run()
        {
            questioner.question();
        }
    }
}
