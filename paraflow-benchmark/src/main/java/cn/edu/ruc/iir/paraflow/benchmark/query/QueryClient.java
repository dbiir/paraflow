package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow benchmark query client
 * args: server type(ap|tp|htap) table [joinTable]
 *
 * @author guodong
 */
public class QueryClient
{
    private QueryClient()
    {}

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

        DBQueryRunner(String serverUrl, String table)
        {
            this.questioner = new DBQuestioner(serverUrl, table);
        }

        @Override
        public void run()
        {
            questioner.question();
        }
    }

    public static void main(String[] args)
    {
        String serverUrl = args[0];
        String mode = args[1];
        String table = args[2];
        String joinTable = "";
        if (args.length == 4) {
            joinTable = args[3];
        }
        switch (mode.toUpperCase()) {
            case "AP":
                PrestoQueryRunner apQueryRunner = new PrestoQueryRunner(serverUrl, table, joinTable);
                new Thread(apQueryRunner).start();
                break;
            case "TP":
                DBQueryRunner tpQueryRunner = new DBQueryRunner(serverUrl, table);
                new Thread(tpQueryRunner).start();
                break;
            case "HTAP":
                PrestoQueryRunner prestoQueryRunner = new PrestoQueryRunner(serverUrl, table, joinTable);
                DBQueryRunner dbQueryRunner = new DBQueryRunner(serverUrl, table);
                new Thread(prestoQueryRunner).start();
                new Thread(dbQueryRunner).start();
                break;
            default:
                System.out.println("No matching mode.");
        }
    }
}
