package cn.edu.ruc.iir.paraflow.commons.func;

import java.io.Serializable;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public interface DeserializableFunction<T, R> extends Function<T, R>, Serializable
{
}
