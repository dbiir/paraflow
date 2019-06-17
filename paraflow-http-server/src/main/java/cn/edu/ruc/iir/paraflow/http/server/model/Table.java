package cn.edu.ruc.iir.paraflow.http.server.model;

import java.util.List;

public class Table extends Base
{
    private static final long serialVersionUID = 8083493591644033068L;
    private String name;
    private List<Column> columns;

    public Table()
    {
    }

    public Table(String name, List<Column> columns)
    {
        this.name = name;
        this.columns = columns;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    @Override
    public String toString()
    {
        return "Table{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                '}';
    }
}
