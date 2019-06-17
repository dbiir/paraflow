package cn.edu.ruc.iir.paraflow.examples.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.loader.DefaultLoader;

/**
 * paraflow
 *
 * @author guodong
 */
public class BasicLoader
{
    private BasicLoader()
    {
    }

    public static void main(String[] args)
    {
        String db = args[0];
        String table = args[1];
        int partitionFrom = Integer.parseInt(args[2]);
        int partitionTo = Integer.parseInt(args[3]);

        try {
            DefaultLoader loader = new DefaultLoader(db, table, partitionFrom, partitionTo);
            loader.run();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
