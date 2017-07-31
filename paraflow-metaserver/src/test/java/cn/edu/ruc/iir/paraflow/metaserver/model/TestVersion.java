package cn.edu.ruc.iir.paraflow.metaserver.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class TestVersion
{
    @Test
    public void versionCreationTest()
    {
        Version v = new Version.Builder()
                .setVersionId(1)
                .save();
        assertEquals(1, v.getVersionId());
    }
}
