package cn.edu.ruc.iir.paraflow.benchmark.query;

/**
 * paraflow benchmark query client
 * args: db-server presto-server type[ap|tp|htap]
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

        PrestoQueryRunner(String serverUrl)
        {
            this.questioner = new PrestoQuestioner(serverUrl);
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

        DBQueryRunner(String serverUrl)
        {
            this.questioner = new DBQuestioner(serverUrl);
        }

        @Override
        public void run()
        {
            questioner.question();
        }
    }

    public static void main(String[] args)
    {
        String dbUrl = args[0];
        String prestoUrl = args[1];
        String mode = args[2];
        PrestoQueryRunner prestoQueryRunner = new PrestoQueryRunner(prestoUrl);
        DBQueryRunner dbQueryRunner = new DBQueryRunner(dbUrl);
        switch (mode.toUpperCase()) {
            case "AP":
                new Thread(prestoQueryRunner).start();
                break;
            case "TP":
                new Thread(dbQueryRunner).start();
                break;
            case "HTAP":
                new Thread(prestoQueryRunner).start();
                new Thread(dbQueryRunner).start();
                break;
            default:
                System.out.println("No matching mode.");
        }
    }
}
