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
    private Utils()
    {}

    public static String formatDbUrl(String dbName) throws FormatUrlException
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
        try {
            return new URI(base + dbName).toString();
        }
        catch (URISyntaxException e) {
            throw new FormatUrlException();
        }
    }
    public static String formatTblUrl(String dbName, String tblName) throws FormatUrlException
    {
        if (dbName.isEmpty() || tblName.isEmpty()) {
            throw new FormatUrlException();
        }
        String base = MetaConfig.INSTANCE().getHDFSWarehouse();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        if (dbName.startsWith("/")) {
            dbName = dbName.substring(1, dbName.length()) + "/";
        }
        if (tblName.startsWith("/")) {
            tblName = tblName.substring(1, tblName.length());
        }
        try {
            return new URI(base + dbName + tblName).toString();
        }
        catch (URISyntaxException e) {
            throw new FormatUrlException();
        }
    }
}
