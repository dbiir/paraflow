package cn.edu.ruc.iir.paraflow.loader;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;

public interface DataTransformer
{
    ParaflowRecord transform(byte[] value, int partition);
}
