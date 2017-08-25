package cn.edu.ruc.iir.paraflow.metaserver.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.FormatUrlException;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * paraflow
 *
 * @author guodong
 */
public class Utils
{
    public static String formatUrl(String dbName) throws FormatUrlException
    {
        if (dbName.isEmpty()) {
            throw new FormatUrlException();
        }
        String base = MetaConfig.INSTANCE().getHDFSWarehouse();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        if (dbName.startsWith("/")) {
            dbName = dbName.substring(1, dbName.length());
        }
        try
        {
            return new URI(base + dbName).toString();
        } catch (URISyntaxException e)
        {
            throw new FormatUrlException();
        }
    }
}
