package cn.edu.ruc.iir.paraflow.http.server.model;

import java.util.ArrayList;
import java.util.List;

public class Json implements java.io.Serializable
{
    private static final long serialVersionUID = 1L;

    private int state = 0; // 是否成功
    private Object datas = ""; // 其他信息
    private String msg = ""; // 其他信息
    private List<String> errorList = new ArrayList<String>(); // 提示信息

    public int getState()
    {
        return state;
    }

    public void setState(int state)
    {
        this.state = state;
    }

    public Object getDatas()
    {
        return datas;
    }

    public void setDatas(Object datas)
    {
        this.datas = datas;
    }

    public String getMsg()
    {
        return msg;
    }

    public void setMsg(String msg)
    {
        this.msg = msg;
    }

    public List<String> getErrorList()
    {
        return errorList;
    }

    public void setErrorList(List<String> errorList)
    {
        this.errorList = errorList;
    }
}
