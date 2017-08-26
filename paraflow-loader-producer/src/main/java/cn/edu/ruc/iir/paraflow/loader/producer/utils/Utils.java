package cn.edu.ruc.iir.paraflow.loader.producer.utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class Utils
{
    private Utils()
    {}

    public static String formTopicName(String database, String table)
    {
        return database + "." + table;
    }
}
