package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * paraflow
 *
 * @author guodong
 */
public class PrestoQuestioner
{
    private static final String DRIVER_CLASS = "com.facebook.presto.jdbc.Driver";
    private final String serverUrl;
    private final PrestoQueryGenerator queryGenerator;
    private final QueryDistribution queryDistribution;

    public PrestoQuestioner(String serverUrl)
    {
        this.serverUrl = serverUrl;
        this.queryDistribution = new QueryDistribution(10, 0, 0, 0);
        queryDistribution.setSizeLimit(1000);
        this.queryGenerator = new PrestoQueryGenerator(queryDistribution);
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
            long startTime = System.currentTimeMillis();
            long queryStart = 0;
            long queryEnd = 0;
            long rsCount = 0;
            while (queryGenerator.hasNext()) {
                queryStart = System.currentTimeMillis();
                String query = queryGenerator.next();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                if (rs.next()) {
                    queryEnd = System.currentTimeMillis();
                    rsCount = rs.getLong("rs_num");
                }
                rs.close();
                stmt.close();
                // todo stats collection
                Thread.sleep(10);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Avg execution latency: " + 1.0d * queryDistribution.sizeLimit() / (endTime - startTime) * 1000);
        }
        catch (ClassNotFoundException | SQLException | InterruptedException e) {
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
        }
    }
}
