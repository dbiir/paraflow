package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * paraflow
 *
 * @author guodong
 */
public interface SegmentCallback
{
    void callback(ReadWriteLock lock);
}
