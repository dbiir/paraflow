package cn.edu.ruc.iir.paraflow.http.server;

import cn.edu.ruc.iir.paraflow.http.server.model.ClusterInfo;
import cn.edu.ruc.iir.paraflow.http.server.model.Pipeline;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
@RestController
@EnableAutoConfiguration
public class PipelineController
{
    private final MetaClient metaClient = new MetaClient("dbiir00", 10012);
    private final Properties properties = new Properties();
    private final String url = "jdbc:presto://dbiir10:8080/";

    public PipelineController()
    {
        properties.setProperty("user", "web");
    }

    @CrossOrigin
    @GetMapping("/info")
    public ClusterInfo info()
    {
        return new ClusterInfo();
    }

    @RequestMapping("/pipeline/p0")
    public String pipeline()
    {
        Pipeline p0 = new Pipeline("p0");
        return p0.toString();
    }

    // list all tables in the test database
    private void listTables()
    {
        MetaProto.StringListType databases = metaClient.listDatabases();
        for (int i = 0; i < databases.getStrCount(); i++) {
            String database = databases.getStr(i);
            MetaProto.StringListType tables = metaClient.listTables(database);
            for (int j = 0; j < tables.getStrCount(); j++) {
                String table = tables.getStr(j);
                MetaProto.StringListType columnIds = metaClient.listColumnsId(database, table);
                MetaProto.StringListType columnNames = metaClient.listColumns(database, table);
                MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(database, table);
            }
        }
    }

    // execute query
    private void executeQuery(String sql)
    {
        try {
            Connection conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // deal with result set

            stmt.close();
            rs.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
