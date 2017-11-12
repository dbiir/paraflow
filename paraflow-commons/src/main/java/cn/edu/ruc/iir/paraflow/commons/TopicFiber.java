package cn.edu.ruc.iir.paraflow.commons;

import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class TopicFiber
{
    private final String topic;
    private final int fiber;

    public TopicFiber(String topic, int fiber)
    {
        this.topic = topic;
        this.fiber = fiber;
    }

    public String getTopic()
    {
        return topic;
    }

    public int getFiber()
    {
        return fiber;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(topic, fiber);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TopicFiber) {
            TopicFiber other = (TopicFiber) obj;
            return Objects.equals(topic, other.topic)
                    && Objects.equals(fiber, other.fiber);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s-%d", topic, fiber);
    }
}
