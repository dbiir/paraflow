package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.RPCServerIOException;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public class StartupPipeline
{
    private final List<StartupHook> hooks;

    public StartupPipeline()
    {
        hooks = new ArrayList<>();
    }

    public void addStartupHook(StartupHook hook)
    {
        hooks.add(hook);
    }

    public void startUp() throws RPCServerIOException
    {
        for (StartupHook hook : hooks) {
            hook.run();
        }
    }
}
