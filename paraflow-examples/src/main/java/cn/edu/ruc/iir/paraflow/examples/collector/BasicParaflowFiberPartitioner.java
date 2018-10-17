package cn.edu.ruc.iir.paraflow.examples.collector;

import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;

import java.nio.ByteBuffer;

/**
 * paraflow
 *
 * @author guodong
 */
public class BasicParaflowFiberPartitioner
        implements ParaflowFiberPartitioner
{
    private final int partitionNum;

    public BasicParaflowFiberPartitioner(int partitionNum)
    {
        this.partitionNum = partitionNum;
    }

    @Override
    public int getFiberId(byte[] key)
    {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        return buffer.getInt() % partitionNum;
    }
}
