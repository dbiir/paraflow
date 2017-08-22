package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;

/**
 * paraflow
 *
 * @author guodong
 */
public class ActionResponse
{
    private int responseInt;
    private ResultList responseResultList;

    public void setResponseInt(int r)
    {
        this.responseInt = r;
    }

    public void setResponseResultList(ResultList res)
    {
        this.responseResultList = res;
    }

    public int getResponseInt()
    {
        return responseInt;
    }

    public ResultList getResponseResultList()
    {
        return responseResultList;
    }
}
