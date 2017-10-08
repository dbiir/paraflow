package cn.edu.ruc.iir.paraflow.loader.producer.buffer;

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
    private final Map<String, Function<String, Long>> functionMap;

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

    public void put(String topic, Function<String, Long> function)
    {
        functionMap.put(topic, function);
    }

    public Optional<Function<String, Long>> get(String topic)
    {
        Function<String, Long> function = functionMap.get(topic);
        if (function == null) {
            return Optional.empty();
        }
        return Optional.of(function);
    }
}
