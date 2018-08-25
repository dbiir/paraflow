package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

    public DBQuestioner(String serverUrl)
    {
        this.serverUrl = serverUrl;
        this.queryDistribution = new QueryDistribution(0, 8, 1, 1);
        queryDistribution.setTimeLimit(60 * 1000);
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
            while (queryGenerator.hasNext()) {
                String query = queryGenerator.next();
                Statement stmt = conn.createStatement();
                int status = stmt.executeUpdate(query);
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
        }
    }
}
