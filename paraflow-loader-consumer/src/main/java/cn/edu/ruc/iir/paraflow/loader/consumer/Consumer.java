package cn.edu.ruc.iir.paraflow.loader.consumer;

import java.util.function.Function;

public interface Consumer
{
    void consume();

    void registerFiberFunc(String database, String table, Function<String, Long> fiberFunc);

    void shutdown();
}
