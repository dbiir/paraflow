package cn.edu.ruc.iir.paraflow.loader;

/**
 * paraflow
 *
 * @author guodong
 */
public interface DataTransformer
{
    ParaflowRecord transform(byte[] value, int partition);
}
