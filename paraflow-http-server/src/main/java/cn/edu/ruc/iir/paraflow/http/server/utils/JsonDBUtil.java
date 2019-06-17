package cn.edu.ruc.iir.paraflow.http.server.utils;

import cn.edu.ruc.iir.paraflow.http.server.model.DynamicJson;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JsonDBUtil
{
    private JsonDBUtil()
    {}

    public static String rSetToJson(ResultSet rs) throws SQLException, JSONException
    {
        JSONArray array = new JSONArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            array.add(jsonObj);
        }
        return array.toString();
    }

    public static JSONArray rSToJson(ResultSet rs) throws SQLException, JSONException
    {
        JSONArray array = new JSONArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            array.add(jsonObj);
        }
        return array;
    }

    public static DynamicJson rSToJson(ResultSet rs, boolean flag) throws SQLException, JSONException
    {
        DynamicJson dynamicJson = new DynamicJson();
        JSONArray array = new JSONArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        dynamicJson.setColCount(columnCount);
        int rowCount = 0;
        String[] column = new String[columnCount];
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
                rowCount++;
                if (1 == i) {
                    column[i - 1] = columnName;
                }
            }
            array.add(jsonObj);
        }
        dynamicJson.setColumn(column);
        dynamicJson.setRowCount(rowCount);
        dynamicJson.setData(array);
        return dynamicJson;
    }
}
