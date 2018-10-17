package cn.edu.ruc.iir.paraflow.http.server;

import cn.edu.ruc.iir.paraflow.http.server.model.ClusterInfo;
import cn.edu.ruc.iir.paraflow.http.server.model.Json;
import cn.edu.ruc.iir.paraflow.http.server.model.Pipeline;
import cn.edu.ruc.iir.paraflow.http.server.utils.JsonDBUtil;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

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
    @RequestMapping(value = "/acGetTables")
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
    @RequestMapping(value = "/acQuery")
    private String executeQuery(@RequestParam("sql") String sql)
    {
        String res = "";
        Json j = new Json();
        try {
            Connection conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // deal with result set
            JSONArray aDBJson = null;
            aDBJson = JsonDBUtil.rSToJson(rs);
            String result = JSON.toJSONString(aDBJson);
            j.setDatas(result);
            j.setState(1);
            stmt.close();
            rs.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        res = JSON.toJSONString(j);
        return res;
    }

    @RequestMapping(value = "/")
    public String getAction() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(url, properties);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from meta_tblmodel"); // meta_dbmodel
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String res = null;
        try {
            res = JsonDBUtil.rSetToJson(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Result is: " + res);
        return res;
    }
}
