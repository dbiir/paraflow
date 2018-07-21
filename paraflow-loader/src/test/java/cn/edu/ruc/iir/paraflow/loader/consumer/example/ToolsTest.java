package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class ToolsTest
{
    private Configuration conf = new Configuration();

    @Test
    public void testSubString()
    {
        String hdfsBasePath = "a/b/";
        if (hdfsBasePath.endsWith("/")) {
            hdfsBasePath = hdfsBasePath.substring(0, hdfsBasePath.length() - 1);
        }
        System.out.println(hdfsBasePath);
    }
}
