package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestConfigFactory
{
    @Test
    public void testConfig()
    {
        ConfigFactory configInstance = ConfigFactory.getConfigInstace("")
                .setDefault("name", "jelly")
                .setDefault("age", "23");
        try {
            configInstance.build();
        }
        catch (ConfigFileNotFoundException e) {
            System.out.println(e);
            assertEquals("jelly", configInstance.getProperty("name"));
            assertEquals("23", configInstance.getProperty("age"));
        }
    }
}
