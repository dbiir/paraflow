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

    public DBQuestioner(String serverUrl)
    {
        this.serverUrl = serverUrl;
        this.queryDistribution = new QueryDistribution();
        queryDistribution.setDistribution("select", 0);
        queryDistribution.setDistribution("insert", 1);
        queryDistribution.setDistribution("update", 9);
        queryDistribution.setDistribution("delete", 0);
        queryDistribution.setTimeLimit(60 * 1000);
        this.latencyCache = new ArrayList<>();
        this.queryGenerator = new DBQueryGenerator(queryDistribution);
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
