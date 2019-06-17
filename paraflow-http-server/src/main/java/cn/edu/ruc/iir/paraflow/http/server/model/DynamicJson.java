package cn.edu.ruc.iir.paraflow.http.server.model;

import java.util.Arrays;

public class DynamicJson
{
    private int rowCount;
    private int colCount;
    private Object data;
    private String[] column;

    public DynamicJson()
    {
    }

    public DynamicJson(int rowCount, int colCount, Object data, String[] column)
    {
        this.rowCount = rowCount;
        this.colCount = colCount;
        this.data = data;
        this.column = column;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(int rowCount)
    {
        this.rowCount = rowCount;
    }

    public int getColCount()
    {
        return colCount;
    }

    public void setColCount(int colCount)
    {
        this.colCount = colCount;
    }

    public Object getData()
    {
        return data;
    }

    public void setData(Object data)
    {
        this.data = data;
    }

    public String[] getColumn()
    {
        return column;
    }

    public void setColumn(String[] column)
    {
        this.column = column;
    }

    @Override
    public String toString()
    {
        return "DynamicJson{" +
                "rowCount=" + rowCount +
                ", colCount=" + colCount +
                ", data=" + data +
                ", column=" + Arrays.toString(column) +
                '}';
    }
}
