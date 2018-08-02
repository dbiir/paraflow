package cn.edu.ruc.iir.paraflow.loader;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class ProcessPipeline
{
    private final List<Processor> processors;

    public ProcessPipeline()
    {
        this.processors = new ArrayList<>();
    }

    public void start()
    {}
}
