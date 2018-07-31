package cn.edu.ruc.iir.paraflow.loader;

import java.util.function.Function;

public interface Consumer
{
    void consume();

    void registerFiberFunc(String database, String table, Function<String, Integer> fiberFunc);

    void shutdown();
}
