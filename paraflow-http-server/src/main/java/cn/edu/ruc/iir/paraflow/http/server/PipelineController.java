package cn.edu.ruc.iir.paraflow.http.server;

import cn.edu.ruc.iir.paraflow.http.server.model.*;
import cn.edu.ruc.iir.paraflow.http.server.utils.JsonDBUtil;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.alibaba.fastjson.JSON;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;

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
    private final MetaClient metaClient = new MetaClient("dbiir00", 10012);
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
            Table tab = new Table();
            List<Column> columnList = new ArrayList<>();
            for (int j = 0; j < tables.getStrCount(); j++) {
                String table = tables.getStr(j);
                tab.setName(table);
                MetaProto.StringListType columnIds = metaClient.listColumnsId(database, table);
                MetaProto.StringListType columnNames = metaClient.listColumns(database, table);
                MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(database, table);
                Column column = new Column();
                column.setName(String.valueOf(columnNames));
                column.setType(String.valueOf(columnTypes));
                column.setId(Integer.valueOf(String.valueOf(columnIds)));
                columnList.add(column);
            }
            tab.setColumns(columnList);
            tableList.add(tab);
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
            ResultSet rs = stmt.executeQuery(sql);

            // deal with result set
            DynamicJson result = JsonDBUtil.rSToJson(rs, true);
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

    @RequestMapping(value = "/ac")
    public String getAction() {
        String res = "";
        Json j = new Json();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(url, properties);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("show schemas"); // meta_dbmodel
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String result = null;
        try {
            result = JsonDBUtil.rSetToJson(rs);
            j.setDatas(result);
            j.setState(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Result is: " + res);
        res = JSON.toJSONString(j);
        return res;
    }


}
