package cn.edu.ruc.iir.paraflow.http.server;

import cn.edu.ruc.iir.paraflow.http.server.model.ClusterInfo;
import cn.edu.ruc.iir.paraflow.http.server.model.Column;
import cn.edu.ruc.iir.paraflow.http.server.model.DynamicJson;
import cn.edu.ruc.iir.paraflow.http.server.model.Json;
import cn.edu.ruc.iir.paraflow.http.server.model.Pipeline;
import cn.edu.ruc.iir.paraflow.http.server.model.Table;
import cn.edu.ruc.iir.paraflow.http.server.utils.JsonDBUtil;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.alibaba.fastjson.JSON;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
@RestController
@EnableAutoConfiguration
public class PipelineController
{
    final MetaClient metaClient = new MetaClient("10.77.110.27", 10012);
    private final Properties properties = new Properties();
    private final String url = "jdbc:presto://dbiir10:8080/paraflow/test";

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
    private String listTables()
    {
        String res = "";
        Json json = new Json();
        List<Table> tableList = new ArrayList<>();
        MetaProto.StringListType databases = metaClient.listDatabases();
        for (int i = 0; i < databases.getStrCount(); i++) {
            String database = databases.getStr(i);
            MetaProto.StringListType tables = metaClient.listTables(database);
            for (int j = 0; j < tables.getStrCount(); j++) {
                Table tab = new Table();
                List<Column> columnList = new ArrayList<>();
                String table = tables.getStr(j);
                tab.setName(table);
                MetaProto.StringListType columnIds = metaClient.listColumnsId(database, table);
                MetaProto.StringListType columnNames = metaClient.listColumns(database, table);
                MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(database, table);
                for (int k = 0; k < columnIds.getStrCount(); k++) {
                    Column column = new Column();
                    column.setName(columnNames.getStr(k));
                    column.setType(columnTypes.getStr(k));
                    column.setId(Integer.valueOf(columnIds.getStr(k)));
                    columnList.add(column);
                }
                tab.setColumns(columnList);
                tableList.add(tab);
            }
        }
        json.setDatas(tableList);
        json.setState(1);
        res = JSON.toJSONString(json);
        return res;
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
            long start = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery(sql);
            long duration = System.currentTimeMillis() - start;

            // deal with result set
            DynamicJson result = JsonDBUtil.rSToJson(rs, true);
            j.setDatas(result);
            j.setState(1);
            j.setMsg("Execution time: " + duration + " ms");
            stmt.close();
            rs.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        res = JSON.toJSONString(j);
        return res;
    }

    @RequestMapping(value = "/ac")
    public String getAction()
    {
        String res = "";
        Json j = new Json();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(url, properties);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("show schemas"); // meta_dbmodel
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        String result = null;
        try {
            result = JsonDBUtil.rSetToJson(rs);
            j.setDatas(result);
            j.setState(1);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Result is: " + res);
        res = JSON.toJSONString(j);
        return res;
    }
}
