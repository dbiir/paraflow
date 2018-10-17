package cn.edu.ruc.iir.paraflow.connector.impl;

import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;

/**
 * paraflow
 *
 * @author guodong
 */
public class DefaultFiberPartitioner
        implements ParaflowFiberPartitioner
{
    @Override
    public int getFiberId(byte[] key)
    {
        return 0;
    }
}
