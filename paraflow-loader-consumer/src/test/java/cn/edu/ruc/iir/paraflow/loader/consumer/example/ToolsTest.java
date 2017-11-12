package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import org.junit.Test;

/**
 * paraflow
 *
 * @author guodong
 */
public class ToolsTest
{
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
