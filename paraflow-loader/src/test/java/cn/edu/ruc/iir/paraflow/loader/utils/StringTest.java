package cn.edu.ruc.iir.paraflow.loader.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * paraflow
 *
 * @author guodong
 */
public class StringTest
{
    @Test
    public void testSubStr()
    {
        String segmentPath = "hdfs://127.0.0.1:9000/paraflow/db/table/file-0";
        int fileNamePoint = segmentPath.lastIndexOf("/");
        int tblPoint = segmentPath.lastIndexOf("/", fileNamePoint - 1);
        int dbPoint = segmentPath.lastIndexOf("/", tblPoint - 1);
        String suffix = segmentPath.substring(dbPoint + 1);
        assertEquals("db/table/file-0", suffix);
    }
}
