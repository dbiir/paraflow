package cn.edu.ruc.iir.paraflow.benchmark.generator;

import java.util.Iterator;

/**
 * paraflow
 *
 * @author guodong
 */
public interface Generator<T>
        extends Iterable<T>
{
    @Override
    Iterator<T> iterator();
}
