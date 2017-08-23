package cn.edu.ruc.iir.paraflow.metaserver.server;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;

/**
 * paraflow
 *
 * @author guodong
 */
public interface StartupHook
{
    void run() throws ParaFlowException;
}
