package cn.edu.ruc.iir.paraflow.commons.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * paraflow
 *
 * @author guodong
 */
public class FiberFuncMapBuffer
{
    private final Map<String, Function<String, Integer>> functionMap;

    private FiberFuncMapBuffer()
    {
        functionMap = new HashMap<>();
    }

    private static class FiberFuncMapBufferHolder
    {
        private static final FiberFuncMapBuffer instance = new FiberFuncMapBuffer();
    }

    public static final FiberFuncMapBuffer INSTANCE()
    {
        return FiberFuncMapBufferHolder.instance;
    }

    public void put(String topic, Function<String, Integer> function)
    {
        functionMap.put(topic, function);
    }

    public Optional<Function<String, Integer>> get(String topic)
    {
        Function<String, Integer> function = functionMap.get(topic);
        if (function == null) {
            return Optional.empty();
        }
        return Optional.of(function);
    }
}
