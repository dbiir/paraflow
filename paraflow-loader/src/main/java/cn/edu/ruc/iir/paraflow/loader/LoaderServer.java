package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

/**
 * paraflow
 *
 * @author guodong
 */
public class LoaderServer
{
    private LoaderServer()
    {}

    public static void main(String[] args)
    {
        String db = args[0];
        String table = args[1];
        int partitionFrom = Integer.parseInt(args[3]);
        int partitionTo = Integer.parseInt(args[4]);

        try {
            DefaultLoader loader = new DefaultLoader(db, table, partitionFrom, partitionTo);
            loader.run();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
