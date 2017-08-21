package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestParaFlowConfig
{
    @Test
    public void testConfig()
    {
        ParaFlowConfig configInstance = ParaFlowConfig.getConfigInstance("")
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
