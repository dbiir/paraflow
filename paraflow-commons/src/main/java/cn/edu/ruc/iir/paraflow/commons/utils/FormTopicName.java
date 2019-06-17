package cn.edu.ruc.iir.paraflow.commons.utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class FormTopicName
{
    private FormTopicName()
    {
    }

    public static String formTopicName(String database, String table)
    {
        return database + "." + table;
    }
}
