package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * paraflow
 *
 * @author guodong
 */
public class ActionResponse
{
    private int responseInt;
    private ResultList responseResultList;
    private Object param = null;
    private Map<String, Object> properties = null;

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

    public void setParam(Object param)
    {
        this.param = param;
    }

    public Optional<Object> getParam()
    {
        if (param == null) {
            return Optional.empty();
        }
        return Optional.of(param);
    }

    public void setProperties(String key, Object value)
    {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, value);
    }

    public Optional<Object> getProperties(String key)
    {
        if (properties == null) {
            return Optional.empty();
        }
        if (properties.get(key) == null) {
            return Optional.empty();
        }
        return Optional.of(properties.get(key));
    }
}
