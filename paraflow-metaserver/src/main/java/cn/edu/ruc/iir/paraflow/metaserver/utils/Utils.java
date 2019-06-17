package cn.edu.ruc.iir.paraflow.metaserver.utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class Utils
{
    private Utils()
    {
    }

    public static String formatDbUrl(String dbName)
    {
        String base = MetaConfig.INSTANCE().getHDFSWarehouse();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        if (dbName.startsWith("/")) {
            dbName = dbName.substring(1, dbName.length());
        }
        return base + dbName;
    }

    public static String formatTblUrl(String dbName, String tblName)
    {
        String base = MetaConfig.INSTANCE().getHDFSWarehouse();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        if (dbName.startsWith("/")) {
            dbName = dbName.substring(1, dbName.length());
        }
        if (tblName.startsWith("/")) {
            tblName = tblName.substring(1, tblName.length());
        }
        return base + dbName + "/" + tblName;
    }
}
