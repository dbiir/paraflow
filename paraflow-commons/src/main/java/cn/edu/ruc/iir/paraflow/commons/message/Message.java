package cn.edu.ruc.iir.paraflow.commons.message;

import java.util.Optional;

public class Message
{
    private final int keyIndex;
    private final String[] values;
    private final long timestamp;
    private String topic;

    public Message(int keyIndex, String[] values)
    {
        this(keyIndex, values, Long.MIN_VALUE);
    }

    public Message(int keyIndex, String[] values, long timestamp)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
    }

    public String getKey()
    {
        return this.values[keyIndex];
    }

    public int getKeyIndex()
    {
        return this.keyIndex;
    }

    public String[] getValues()
    {
        return this.values;
    }

    public Optional<Long> getTimestamp()
    {
        if (timestamp != Long.MIN_VALUE) {
            return Optional.of(timestamp);
        }
        return Optional.empty();
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public Optional<String> getTopic()
    {
        if (topic != null) {
            return Optional.of(topic);
        }
        return Optional.empty();
    }

    // todo override equals, hashCode and toString
}
