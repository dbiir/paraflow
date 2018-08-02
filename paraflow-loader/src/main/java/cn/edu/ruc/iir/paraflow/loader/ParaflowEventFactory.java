package cn.edu.ruc.iir.paraflow.loader;

import com.lmax.disruptor.EventFactory;

/**
 * paraflow
 *
 * @author guodong
 */
public class ParaflowEventFactory
        implements EventFactory<ParaflowRecord>
{
    @Override
    public ParaflowRecord newInstance()
    {
        return new ParaflowRecord();
    }
}
