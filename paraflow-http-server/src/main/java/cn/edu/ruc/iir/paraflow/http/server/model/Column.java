package cn.edu.ruc.iir.paraflow.http.server.model;

public class Column extends Base
{
    private static final long serialVersionUID = 7838057854561622243L;
    private String name;
    private String type;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }
}
