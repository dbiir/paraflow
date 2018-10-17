package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * paraflow
 *
 * @author guodong
 */
public class DBQuestioner
{
    private static final String DRIVER_CLASS = "org.postgresql.Driver";
    private final String serverUrl;
    private final DBQueryGenerator queryGenerator;
    private final QueryDistribution queryDistribution;
    private final List<Integer> latencyCache;

    public DBQuestioner(String serverUrl, String table, String mode)
    {
        this.serverUrl = serverUrl;
        this.queryDistribution = new QueryDistribution();
        if (mode.equalsIgnoreCase("u")) {
            queryDistribution.setDistribution("select", 0);
            queryDistribution.setDistribution("insert", 0);
            queryDistribution.setDistribution("update", 100);
            queryDistribution.setDistribution("delete", 0);
        }
        if (mode.equalsIgnoreCase("ui")) {
            queryDistribution.setDistribution("select", 0);
            queryDistribution.setDistribution("insert", 5);
            queryDistribution.setDistribution("update", 95);
            queryDistribution.setDistribution("delete", 0);
        }
        if (mode.equalsIgnoreCase("uid")) {
            queryDistribution.setDistribution("select", 0);
            queryDistribution.setDistribution("insert", 5);
            queryDistribution.setDistribution("update", 90);
            queryDistribution.setDistribution("delete", 5);
        }

        queryDistribution.setTimeLimit(60 * 1000);
        this.latencyCache = new ArrayList<>();
        this.queryGenerator = new DBQueryGenerator(queryDistribution, table);
    }

    public void question()
    {
        Connection conn = null;
        Properties properties = new Properties();
        properties.setProperty("user", "paraflow");
        properties.setProperty("password", "paraflow");
        try {
            Class.forName(DRIVER_CLASS);
            conn = DriverManager.getConnection(serverUrl, properties);
            int counter = 0;
            long queryStart;
            long queryEnd;
            while (queryGenerator.hasNext()) {
                queryStart = System.currentTimeMillis();
                String query = queryGenerator.next();
                Statement stmt = conn.createStatement();
                stmt.executeUpdate(query);
                queryEnd = System.currentTimeMillis();
                latencyCache.add((int) (queryEnd - queryStart));
                counter++;
                stmt.close();
            }
            System.out.println("Throughput: " + (counter * 1.0d / this.queryDistribution.timeLimit() * 1000.0) + " t/s");
        }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            printStats();
        }
    }

    private void printStats()
    {
        int sum = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int latency : latencyCache) {
            sum = sum + latency;
            if (latency < min) {
                min = latency;
            }
            if (latency > max) {
                max = latency;
            }
        }
        double avg = 1.0d * sum / latencyCache.size();
        System.out.println("Avg: " + avg + "ms, min: " + min + "ms, max: " + max + "ms.");
    }
}
